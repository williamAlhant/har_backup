use clap::{Parser, Args, Subcommand};
use anyhow::{Result, Context};
use har_backup::{blob_storage_local_directory::BlobStorageLocalDirectory, mirror::Mirror};
use har_backup::blob_storage::BlobStorage;
use std::path::{Path, PathBuf};
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
        _ => todo!()
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

struct WithRemoteAndLocal {
    local_meta: dot_har::DotHar,
    remote: Option<Mirror>,
}

impl WithRemoteAndLocal {
    fn new() -> Result<Self> {
        let local_meta = dot_har::DotHar::find_cwd_or_ancestor()?;
        let mut me = Self {
            local_meta,
            remote: None
        };
        me.remote = Some(me.init_mirror()?);
        Ok(me)
    }

    fn fetch_manifest(&mut self) -> Result<()> {
        let remote = self.remote.as_mut().unwrap();
        let manifest_blob = remote.get_manifest_blob()?;
        self.local_meta.store_manifest(manifest_blob)?;
        println!("Fetched manifest.");
        Ok(())
    }

    fn init_mirror(&self) -> Result<Mirror> {
        let blob_storage = self.init_blob_storage()?;
        let mirror = Mirror::new(blob_storage);
        Ok(mirror)
    }

    fn init_blob_storage(&self) -> Result<Box<dyn BlobStorage>> {

        let keypath = self.local_meta.get_key_file()?;

        if !keypath.exists() {
            anyhow::bail!("Keyfile {} (as specified by .har) not found", keypath.to_str().unwrap());
        }

        let remote_spec = self.local_meta.get_remote_spec()?;
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
}

const DOT_HAR_NAME: &str = ".har";
mod dot_har {
    use std::path::{Path, PathBuf};
    use super::DOT_HAR_NAME;
    use anyhow::{Result, Context, anyhow};

    const KEYPATH_FILE: &str = "keypath";
    const REMOTE_FILE: &str = "remote";
    const FETCHED_MANIFEST: &str = "fetched_manifest";

    pub struct DotHar {
        path: PathBuf
    }

    impl DotHar {
        pub fn find_cwd_or_ancestor() -> Result<Self> {
            let cwd = std::env::current_dir()?;
            for dir in cwd.ancestors() {
                let maybe_exists = dir.join(DOT_HAR_NAME);
                if maybe_exists.exists() {
                    return Ok(Self{path: maybe_exists});
                }
            }
            anyhow::bail!("Did not find {} in cwd or any ancestor dir", DOT_HAR_NAME)
        }

        pub fn get_key_file(&self) -> Result<PathBuf> {
            let file_content = self.read_file(KEYPATH_FILE)?;
            let keypath_str = String::from_utf8(file_content)?;
            Ok(PathBuf::from(&keypath_str))
        }

        pub fn get_remote_spec(&self) -> Result<String> {
            let file_content = self.read_file(REMOTE_FILE)?;
            let remote_spec = String::from_utf8(file_content)?;
            Ok(remote_spec)
        }

        fn read_file(&self, name: &str) -> Result<Vec<u8>> {
            let file = self.path.join(name);
            let file_content = std::fs::read(&file).with_context(|| anyhow!("Read {}", file.to_str().unwrap()))?;
            Ok(file_content)
        }

        pub fn store_manifest(&self, manifest_blob: bytes::Bytes) -> Result<()> {
            std::fs::write(self.path.join(FETCHED_MANIFEST), &manifest_blob).context("Storing fetched manifest")?;
            Ok(())
        }
    }
}

fn init_local() -> Result<()> {
    if Path::new(DOT_HAR_NAME).exists() {
        anyhow::bail!("It looks like this has been initialized already!")
    }
    std::fs::create_dir(DOT_HAR_NAME)?;
    println!("Archive initialized.");
    Ok(())
}
