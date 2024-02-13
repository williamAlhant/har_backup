use har_backup::mirror::Mirror;
use anyhow::Result;

mod blob_storage;
use blob_storage::make_dummy_blob_storage;

#[test]
fn init() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("create tempdir for local blob storage");
    let blob_storage = make_dummy_blob_storage(tempdir.path());
    let mut mirror = Mirror::new(Box::new(blob_storage));
    mirror.init()?;
    Ok(())
}

#[test]
fn init_twice() -> Result<()> {
    let tempdir = tempfile::tempdir().expect("create tempdir for local blob storage");
    let blob_storage = make_dummy_blob_storage(tempdir.path());
    let mut mirror = Mirror::new(Box::new(blob_storage));
    mirror.init()?;
    mirror.init()?;
    Ok(())
}