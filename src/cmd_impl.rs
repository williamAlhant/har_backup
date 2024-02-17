use anyhow::{Result, Context};
use crate::manifest::{self, Manifest};
use crate::mirror::TransferConfig;
use crate::{blob_storage_local_directory::BlobStorageLocalDirectory, mirror::Mirror};
use crate::blob_storage::BlobStorage;
use crate::dot_har::DotHar;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use log::debug;

pub struct WithLocal {
    local_meta: DotHar,
}

impl WithLocal {
    pub fn new() -> Result<Self> {
        let local_meta = DotHar::find_cwd_or_ancestor()?;
        let me = Self {
            local_meta,
        };
        Ok(me)
    }

    pub fn diff(&self, remote: bool) -> Result<()> {
        let local_manifest = Manifest::from_fs(self.local_meta.get_archive_root()).context("Making manifest from local tree")?;
        let remote_manifest = self.local_meta.get_manifest().context("Reading fetched manifest")?;
        let diff = match remote {
            false => manifest::diff_manifests(&local_manifest, &remote_manifest),
            true => manifest::diff_manifests(&remote_manifest, &local_manifest),
        };

        if remote {
            println!("Remote has the additional entries:");
        }
        else {
            println!("Local tree has the additional entries:");
        }
        for entry_path in &diff.paths_of_top_extra_in_a {
            println!("{}", entry_path.to_str().unwrap());
        }
        println!("Total extra files: {}, total extra dirs: {}", diff.extra_files_in_a, diff.extra_dirs_in_a);
        Ok(())
    }

    pub fn print_fetched_manifest(&self) -> Result<()> {
        let fetched_manifest = self.local_meta.get_manifest().context("Reading fetched manifest")?;
        let stats = fetched_manifest.get_stats();
        println!("{:?}", stats);
        manifest::print_tree(&fetched_manifest);
        Ok(())
    }
}

pub struct WithRemoteAndLocal {
    local_meta: DotHar,
    remote: Mirror,
}

impl WithRemoteAndLocal {
    pub fn new() -> Result<Self> {
        let local_meta = DotHar::find_cwd_or_ancestor()?;
        let remote = Self::init_mirror(&local_meta)?;
        let me = Self {
            local_meta,
            remote
        };
        Ok(me)
    }

    pub fn fetch_manifest(&mut self) -> Result<()> {
        let manifest_blob = self.remote.get_manifest_blob()?;
        self.local_meta.store_manifest(manifest_blob)?;
        println!("Fetched manifest.");
        Ok(())
    }

    pub fn init_remote(&mut self) -> Result<()> {
        self.remote.init()?;
        println!("Remote initialized.");
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

    pub fn push(&mut self) -> Result<()> {
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
        let results = self.remote.push(&paths_in_archive, prefix_path, TransferConfig::default())?;
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

    pub fn pull(&mut self) -> Result<()> {
        let local_manifest = Manifest::from_fs(self.local_meta.get_archive_root()).context("Making manifest from local tree")?;
        let remote_manifest = self.local_meta.get_manifest().context("Reading fetched manifest")?;
        let diff = manifest::diff_manifests(&remote_manifest, &local_manifest);

        if diff.top_extra_ids_in_a.is_empty() {
            println!("Nothing to pull.");
            return Ok(());
        }

        // println!("Starting to pull {} files...", files_to_push.len());
        // let results = self.remote.pull(&paths_in_archive, prefix_path, PushConfig::default())?;
        // println!("Pull done.");
        todo!()
    }
}

pub mod for_integ_test {
    use std::path::Path;
    use super::{WithLocal, WithRemoteAndLocal};
    use super::DotHar;
    pub fn with_local(dot_har_path: &Path) -> WithLocal {
        WithLocal { local_meta: DotHar::with_path(dot_har_path.to_path_buf()) }
    }
    pub fn with_remote_and_local(dot_har_path: &Path) -> WithRemoteAndLocal {
        let local_meta = DotHar::with_path(dot_har_path.to_path_buf());
        let remote = WithRemoteAndLocal::init_mirror(&local_meta).unwrap();
        WithRemoteAndLocal {
            local_meta,
            remote
        }
    }
}