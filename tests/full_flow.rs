use anyhow::{Result, Context};
use tempfile::TempDir;
use std::path::{Path, PathBuf};

use har_backup::cmd_impl::{WithLocal, WithRemoteAndLocal};
use har_backup::dot_har::{DotHar, DOT_HAR_NAME};

fn create_key(path: &Path) -> Result<()> {
    let key = har_backup::blob_encryption::create_key();
    std::fs::write(path, key.as_slice()).context("Writing key to file")?;
    Ok(())
}

fn make_dummy_archive() -> (TempDir, TempDir, PathBuf) {
    let archive_root = TempDir::new().unwrap();
    let dot_har_path = archive_root.path().join(DOT_HAR_NAME);
    std::fs::create_dir(&dot_har_path).unwrap();
    let dot_har = DotHar::with_path(dot_har_path.clone());

    let storage = TempDir::new().unwrap();
    let remote_spec = format!("fs://{}", storage.path().to_str().unwrap());
    dot_har.set_remote_spec(&remote_spec).unwrap();

    let key_path = dot_har_path.join("kek_keyfile");
    create_key(&key_path).unwrap();
    dot_har.set_path_to_keyfile(&key_path).unwrap();

    (archive_root, storage, dot_har_path)
}

#[test]
fn fetch_diff_push() -> Result<()> {
    let (archive_root, storage, dot_har_path) = make_dummy_archive();
    let mut with_remote_and_local = har_backup::cmd_impl::for_integ_test::with_remote_and_local(&dot_har_path);
    let with_local = har_backup::cmd_impl::for_integ_test::with_local(&dot_har_path);

    with_remote_and_local.init_remote()?;
    with_remote_and_local.fetch_manifest()?;
    with_local.diff(false, false)?;

    let new_file_path = archive_root.path().join("chuchu");
    std::fs::write(&new_file_path, "tamtam").unwrap();

    with_remote_and_local.push()?;

    Ok(())
}