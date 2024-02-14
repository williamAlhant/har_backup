use har_backup::{blob_storage_local_directory::BlobStorageLocalDirectory, mirror::Mirror};
use std::path::Path;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("Hello, world!");

    let blob_storage = BlobStorageLocalDirectory::new(Path::new("local_storage"), Path::new("test_files/keyfile"))?;
    let mut mirror = Mirror::new(Box::new(blob_storage));
    // mirror.init()?;

    mirror.diff_with_local(Path::new("test_files/partial_h_archive"))?;

    Ok(())
}
