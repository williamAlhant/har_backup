use std::path::Path;
use anyhow::Context;
use log::debug;
use har_backup::blob_storage_local_directory::BlobStorageLocalDirectory;
use har_backup::blob_storage::BlobStorage;
use har_backup::blob_storage::EventContent;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("Hello, world!");
    test_download()
}

fn make_blob_storage() -> anyhow::Result<BlobStorageLocalDirectory> {
    BlobStorageLocalDirectory::new(Path::new("local_storage"), Path::new("/home/cookie/code/har_backup/test_files/keyfile"))
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
    blob_storage.download("e40bf87e3889651df869f277a3fe25d8a81043781b9d4c21e2b32a9c9f182907");
    std::thread::sleep(std::time::Duration::from_millis(100));

    match events.try_recv() {
        Ok(ev) => {
            debug!("try_recv ev {}", ev);
            if let EventContent::DownloadSuccess(mut bytes) = ev.content {
                bytes.truncate(16);
                debug!("bytes: {:?}(...)", bytes);
            }
        },
        Err(recv_err) => {
            debug!("try_recv err {}", recv_err);
        }
    }

    Ok(())
}
