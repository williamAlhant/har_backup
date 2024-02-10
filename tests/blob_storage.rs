use har_backup::blob_storage::{BlobStorage, EventContent};
use har_backup::blob_storage_local_directory::BlobStorageLocalDirectory;
use std::io::Write;

#[test]
fn local_directory_upload_and_download() {

    let tempdir = tempfile::tempdir().expect("create tempdir for local blob storage");
    let mut keyfile = tempfile::NamedTempFile::new().expect("create tempfile for dummy encryption key");
    let key: [u8; 32] = [1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
    keyfile.write_all(&key).expect("write key file content");
    let dummy_payload = bytes::Bytes::from("Hello I am a dummy payload");

    let mut blob_storage = BlobStorageLocalDirectory::new(tempdir.path(), keyfile.path()).expect("create blob storage");

    let events = blob_storage.events();

    blob_storage.upload(dummy_payload.clone());

    let event = events.recv().expect("receive an event for upload");
    let blob_hash = match event.content {
        EventContent::UploadSuccess(blob_hash) => blob_hash,
        _ => panic!("Expected UploadSuccess")
    };

    blob_storage.download(&blob_hash);

    let event = events.recv().expect("receive an event for download");
    let bytes = match event.content {
        EventContent::DownloadSuccess(bytes) => bytes,
        _ => panic!("Expected DownloadSuccess")
    };

    assert_eq!(dummy_payload, bytes);
}