use std::path::{Path, PathBuf};
use anyhow::Context;
use log::debug;
use har_backup::blob_storage_local_directory::BlobStorageLocalDirectory;
use har_backup::blob_storage::BlobStorage;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("Hello, world!");
    test_download()
}

fn make_blob_storage() -> anyhow::Result<BlobStorageLocalDirectory> {
    let dummy_key_path = PathBuf::from("/home/cookie/code/har_backup/test_files/keyfile");
    BlobStorageLocalDirectory::new(Path::new("local_storage"), &dummy_key_path)
}

fn test_upload() -> anyhow::Result<()> {
    let mut blob_storage = make_blob_storage()?;
    let filecontent = std::fs::read(Path::new("test_files/yolo"))?;
    let events = blob_storage.events();
    blob_storage.upload(bytes::Bytes::from(filecontent));
    std::thread::sleep(std::time::Duration::from_millis(100));

    match events.try_recv() {
        Ok(ev) => {
            debug!("try_recv ev {}", ev);
        },
        Err(recv_err) => {
            debug!("try_recv err {}", recv_err);
        }
    }

    Ok(())
}

fn test_download() -> anyhow::Result<()> {
    let mut blob_storage = make_blob_storage()?;

    let events = blob_storage.events();
    blob_storage.download("242812574eefce8623b6bdfdf3531738928728f608a6512f494c8aee4ec29c01");
    std::thread::sleep(std::time::Duration::from_millis(100));

    match events.try_recv() {
        Ok(ev) => {
            debug!("try_recv ev {}", ev);
        },
        Err(recv_err) => {
            debug!("try_recv err {}", recv_err);
        }
    }

    Ok(())
}