
use std::path::{Path, PathBuf};
use anyhow::{Result, Context, anyhow};
use super::manifest::Manifest;
use std::ops::Range;

pub const DOT_HAR_NAME: &str = ".har";
const KEYPATH_FILE: &str = "keypath";
const REMOTE_FILE: &str = "remote";
const FETCHED_MANIFEST: &str = "fetched_manifest";
const FETCHED_MANIFEST_BACKUP: &str = "fetched_manifest.backup";

#[derive(Clone)]
pub struct DotHar {
    path: PathBuf
}

pub enum RemoteSpec {
    LocalFileSystem(PathBuf),
    S3(S3Spec),
}

pub struct S3Spec {
    underlying: String,
    endpoint: Range<usize>,
    bucket_name: Range<usize>,
    key: Range<usize>,
    secret: Range<usize>,
}

impl S3Spec {
    pub fn endpoint(&self) -> &str {
        &self.underlying.as_str()[self.endpoint.clone()]
    }
    pub fn bucket_name(&self) -> &str {
        &self.underlying.as_str()[self.bucket_name.clone()]
    }
    pub fn key(&self) -> &str {
        &self.underlying.as_str()[self.key.clone()]
    }
    pub fn secret(&self) -> &str {
        &self.underlying.as_str()[self.secret.clone()]
    }
}

impl RemoteSpec {
    fn parse(spec_str: &str) -> Result<Self> {
        let (scheme, the_rest) = spec_str.split_once("://").context("Remote spec (as specified by .har) does not have format A://B")?;
        let ret = match scheme {
            "fs" => {
                RemoteSpec::LocalFileSystem(PathBuf::from(the_rest))
            },
            "s3" => {
                let mut lines = the_rest.lines();
                let mut underlying = String::new();

                let mut get_line_and_push_underlying = || -> anyhow::Result<_> {
                    let line = lines.next().context("Parsing s3 spec in .har")?;
                    let range = underlying.len()..(underlying.len() + line.len());
                    underlying.push_str(line);
                    Ok(range)
                };

                let endpoint = get_line_and_push_underlying()?;
                let bucket_name = get_line_and_push_underlying()?;
                let key = get_line_and_push_underlying()?;
                let secret = get_line_and_push_underlying()?;

                let s3_spec = S3Spec {
                    underlying,
                    endpoint,
                    bucket_name,
                    key,
                    secret,
                };
                RemoteSpec::S3(s3_spec)
            },
            _ => anyhow::bail!("Unknown scheme {}", scheme)
        };
        Ok(ret)
    }
}

impl DotHar {

    // should be used for testing only
    pub fn with_path(path: PathBuf) -> Self {
        Self { path }
    }

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

    pub fn get_remote_spec(&self) -> Result<RemoteSpec> {
        let file_content = self.read_file(REMOTE_FILE)?;
        let remote_spec = String::from_utf8(file_content)?;
        let remote_spec = RemoteSpec::parse(&remote_spec)?;
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

    pub fn set_path_to_keyfile(&self, path: &Path) -> Result<()> {
        std::fs::write(self.path.join(KEYPATH_FILE), path.to_str().context("Path to str")?).context("Write KEYPATH_FILE")
    }

    pub fn set_remote_spec(&self, spec: &str) -> std::io::Result<()> {
        std::fs::write(self.path.join(REMOTE_FILE), spec)
    }
}
