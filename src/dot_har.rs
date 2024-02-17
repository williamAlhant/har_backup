
use std::path::{Path, PathBuf};
use anyhow::{Result, Context, anyhow};
use super::manifest::Manifest;

pub const DOT_HAR_NAME: &str = ".har";
const KEYPATH_FILE: &str = "keypath";
const REMOTE_FILE: &str = "remote";
const FETCHED_MANIFEST: &str = "fetched_manifest";
const FETCHED_MANIFEST_BACKUP: &str = "fetched_manifest.backup";

#[derive(Clone)]
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

    pub fn get_archive_root(&self) -> &Path {
        self.path.parent().unwrap()
    }

    pub fn get_manifest(&self) -> Result<Manifest> {
        let file_content = self.read_file(FETCHED_MANIFEST)?;
        let manifest = Manifest::from_bytes(bytes::Bytes::from(file_content))?;
        Ok(manifest)
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

    pub fn store_manifest_with_backup(&self, manifest_blob: bytes::Bytes) -> Result<()> {
        let path = self.path.join(FETCHED_MANIFEST);
        let backup_path = self.path.join(FETCHED_MANIFEST_BACKUP);
        std::fs::copy(&path, backup_path).context("Backup of fetched manifest")?;
        std::fs::write(path, &manifest_blob).context("Storing fetched manifest")?;
        Ok(())
    }
}
