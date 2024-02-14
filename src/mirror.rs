use super::blob_storage::BlobStorage;
use super::manifest::{Manifest, diff_manifests};
use std::path::Path;
use log::debug;

pub struct Mirror {
    blob_storage: Box<dyn BlobStorage>
}

const MANIFEST_KEY: &str = "manifest";

impl Mirror {
    pub fn new(blob_storage: Box<dyn BlobStorage>) -> Self {
        Self {
            blob_storage
        }
    }

    // like git init; create/upload an empty remote manifest
    pub fn init(&mut self) -> anyhow::Result<()> {

        let exists = self.blob_storage.exists_blocking(MANIFEST_KEY)?;
        if exists {
            anyhow::bail!("Manifest already exists in remote");
        }

        let manifest = Manifest::new();
        let data = manifest.to_bytes()?;
        self.blob_storage.upload_blocking(data, Some(MANIFEST_KEY))?;
        Ok(())
    }

    // todo make private
    pub fn diff_with_local(&mut self, local_dir: &Path) -> anyhow::Result<()> {
        debug!("Get manifest from local fs...");
        let local_manifest = Manifest::from_fs(local_dir)?;
        debug!("Get manifest from local fs done");
        debug!("Download remote manifest...");
        let remote_manifest_bytes = self.blob_storage.download_blocking(MANIFEST_KEY)?;
        debug!("Download remote manifest done");
        let remote_manifest = Manifest::from_bytes(remote_manifest_bytes)?;
        let diff = diff_manifests(&local_manifest, &remote_manifest);
        debug!("{}", diff);
        Ok(())
    }
}