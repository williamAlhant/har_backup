use clap::{Parser, Args, Subcommand};
use anyhow::{Result, Context};
use har_backup::manifest::{self, print_tree, Manifest};
use har_backup::mirror::PushConfig;
use har_backup::{blob_storage_local_directory::BlobStorageLocalDirectory, mirror::Mirror};
use har_backup::blob_storage::{BlobStorage, UploadResult};
use har_backup::dot_har::DotHar;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use log::debug;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(
        about="Create an encryption key",
        after_help="The key is used to encrypt/decrypt blobs. It is up to you to store it safely.",
    )]
    CreateKey(CreateKey),
    #[command(
        about="Initialize the local archive directory",
        after_help="It makes the current working directory the archive root.\n\
                    It creates a .har directory containing config/metadata",
    )]
    InitLocal,
    #[command(
        about="Fetch the remote manifest",
        after_help="It stores the manifest in .har",
    )]
    FetchManifest,
    #[command(
        about="Compare local tree with fetched manifest",
        after_help="Do not forget to fetch before.",
    )]
    Diff,
    #[command(
        about="Push changes from local to remote",
        after_help="It diffs local tree with fetched remote manifest.\n\
                    It uploads new files, directories and uploads the updated manifest.",
    )]
    Push
}

#[derive(Args, Debug)]
struct CreateKey {
    path: PathBuf,
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    match cli.command {
        Command::CreateKey(sub_cli) => create_key(&sub_cli.path),
        Command::InitLocal => init_local(),
        Command::FetchManifest => WithRemoteAndLocal::new()?.fetch_manifest(),
        Command::Diff => WithLocal::new()?.diff(),
        Command::Push => WithRemoteAndLocal::new()?.push(),
    }
}

fn write_file_without_overwrite(path: &Path, content: &[u8]) -> Result<()> {
    if path.exists() {
        anyhow::bail!("{} already exists", path.to_str().unwrap());
    }
    std::fs::write(path, content)?;
    Ok(())
}

fn create_key(path: &Path) -> Result<()> {
    let path_str = path.to_str().context("Convert path to str")?;
    println!("Creating key");
    let key = har_backup::blob_encryption::create_key();
    write_file_without_overwrite(path, key.as_slice()).context("Writing key to file")?;
    println!("key stored at {}", path_str);
    Ok(())
}

struct WithLocal {
    local_meta: DotHar,
}

impl WithLocal {
    fn new() -> Result<Self> {
        let local_meta = DotHar::find_cwd_or_ancestor()?;
        let me = Self {
            local_meta,
        };
        Ok(me)
    }

    fn diff(&self) -> Result<()> {
        let local_manifest = Manifest::from_fs(self.local_meta.get_archive_root()).context("Making manifest from local tree")?;
        let remote_manifest = self.local_meta.get_manifest().context("Reading fetched manifest")?;
        let diff = manifest::diff_manifests(&local_manifest, &remote_manifest);
        println!("Local tree has the additional entries:");
        for entry_path in &diff.paths_of_top_extra_in_a {
            println!("{}", entry_path.to_str().unwrap());
        }
        println!("Total extra files: {}, total extra dirs: {}", diff.extra_files_in_a, diff.extra_dirs_in_a);
        Ok(())
    }
}

struct WithRemoteAndLocal {
    local_meta: DotHar,
    remote: Mirror,
}

impl WithRemoteAndLocal {
    fn new() -> Result<Self> {
        let local_meta = DotHar::find_cwd_or_ancestor()?;
        let remote = Self::init_mirror(&local_meta)?;
        let me = Self {
            local_meta,
            remote
        };
        Ok(me)
    }

    fn fetch_manifest(&mut self) -> Result<()> {
        let manifest_blob = self.remote.get_manifest_blob()?;
        self.local_meta.store_manifest(manifest_blob)?;
        println!("Fetched manifest.");
        Ok(())
    }

    fn init_mirror(local_meta: &DotHar) -> Result<Mirror> {
        let blob_storage = Self::init_blob_storage(local_meta)?;
        let mirror = Mirror::new(blob_storage);
        Ok(mirror)
    }

    fn init_blob_storage(local_meta: &DotHar) -> Result<Box<dyn BlobStorage>> {

        let keypath = local_meta.get_key_file()?;

        if !keypath.exists() {
            anyhow::bail!("Keyfile {} (as specified by .har) not found", keypath.to_str().unwrap());
        }

        let remote_spec = local_meta.get_remote_spec()?;
        let (scheme, path) = remote_spec.split_once("://").context("Remote spec (as specified by .har) does not have format A://B")?;
        debug!("Remote scheme/path {} {}", scheme, path);

        if scheme == "fs" {
            let blob_storage = BlobStorageLocalDirectory::new(Path::new(path), &keypath)?;
            Ok(Box::new(blob_storage))
        }
        else {
            todo!();
        }
    }

    fn push(&mut self) -> Result<()> {
        let local_manifest = Manifest::from_fs(self.local_meta.get_archive_root()).context("Making manifest from local tree")?;
        let mut remote_manifest = self.local_meta.get_manifest().context("Reading fetched manifest")?;
        let diff = manifest::diff_manifests(&local_manifest, &remote_manifest);

        if diff.top_extra_ids_in_a.is_empty() {
            println!("Nothing to push.");
            return Ok(());
        }

        let path_getter = local_manifest.get_full_path_getter();

        let mut files_to_push = Vec::new();
        for &top_extra_entry in &diff.top_extra_ids_in_a {
            let extra_files = local_manifest.get_child_files_recurs(top_extra_entry);
            files_to_push.extend(extra_files);
        }
        let paths_in_archive: Vec<PathBuf> = files_to_push.iter().map(|&id| path_getter(id)).collect();
        let prefix_path = self.local_meta.get_archive_root();

        println!("Starting to push {} files...", files_to_push.len());
        let results = self.remote.push(&paths_in_archive, prefix_path, PushConfig::default())?;
        println!("Push done. Next is to update the remote manifest.");

        // for testing
        // let results = vec![Some(UploadResult::Ok("05fd1dcbe8e3b2932f532f1c35b25607ad697b122245829b090178e645223ac1".to_string())); paths_in_archive.len()];

        let mut blob_keys: HashMap<PathBuf, String> = HashMap::with_capacity(results.len());
        for (path, result) in std::iter::zip(paths_in_archive, results){
            let result = result.context("Result of upload not filled properly")?;
            let hash_str = result.context("Result of upload is error")?;
            blob_keys.insert(path, hash_str);
        }

        manifest::add_new_entries_to_manifest(&local_manifest, &mut remote_manifest, &diff, &blob_keys)?;
        debug!("add_new_entries_to_manifest done");

        let new_remote_manifest_bytes = remote_manifest.to_bytes()?;
        self.remote.push_manifest_blob(new_remote_manifest_bytes.clone())?;
        debug!("Upload of new manifest done");

        self.local_meta.store_manifest_with_backup(new_remote_manifest_bytes)?;
        debug!("New manifest stored");

        println!("Remote manifest updated.");

        Ok(())
    }
}

fn init_local() -> Result<()> {
    use har_backup::dot_har::DOT_HAR_NAME;
    if Path::new(DOT_HAR_NAME).exists() {
        anyhow::bail!("It looks like this has been initialized already!")
    }
    std::fs::create_dir(DOT_HAR_NAME)?;
    println!("Archive initialized.");
    Ok(())
}
