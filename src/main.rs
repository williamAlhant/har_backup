use std::path::Path;
use anyhow::Context;
use log::debug;
use har_backup::blob_storage_local_directory::BlobStorageLocalDirectory;
use har_backup::blob_storage::BlobStorage;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    println!("Hello, world!");
    let mut blob_storage = BlobStorageLocalDirectory::new(Path::new("local_storage"))?;
    let filehandle = std::fs::File::open(Path::new("test_files/yolo"))?;
    let events = blob_storage.events();
    blob_storage.upload(filehandle);
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
