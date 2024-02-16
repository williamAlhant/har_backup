use super::blob_storage::BlobStorage;
use super::manifest::Manifest;
use log::debug;
use anyhow::Result;

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

    pub fn get_manifest_blob(&mut self) -> Result<bytes::Bytes> {
        debug!("Download remote manifest...");
        let remote_manifest_bytes = self.blob_storage.download_blocking(MANIFEST_KEY)?;
        debug!("Download remote manifest done");
        Ok(remote_manifest_bytes)
    }
}