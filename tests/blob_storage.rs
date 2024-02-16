use har_backup::blob_storage::{BlobStorage, EventContent};
use har_backup::blob_storage_local_directory::BlobStorageLocalDirectory;
use har_backup::blob_encryption::EncryptWithChacha;
use tempfile::NamedTempFile;
use std::io::Write;
use anyhow::Result;
use std::path::Path;

pub fn make_dummy_keyfile() -> NamedTempFile {
    let mut keyfile = tempfile::NamedTempFile::new().expect("create tempfile for dummy encryption key");
    let key: [u8; 32] = [1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
    keyfile.write_all(&key).expect("write key file content");
    keyfile
}

pub fn make_dummy_blob_storage(dirpath: &Path) -> BlobStorageLocalDirectory {
    let keyfile = make_dummy_keyfile();
    BlobStorageLocalDirectory::new(dirpath, keyfile.path()).expect("create blob storage")
}

#[test]
fn local_directory_upload_and_download() -> Result<()> {

    let tempdir = tempfile::tempdir().expect("create tempdir for local blob storage");
    let mut blob_storage = make_dummy_blob_storage(tempdir.path());
    let events = blob_storage.events();

    let dummy_payload = bytes::Bytes::from("Hello I am a dummy payload");

    blob_storage.upload(dummy_payload.clone(), None);

    let event = events.recv().expect("receive an event for upload");
    let blob_hash = match event.content {
        EventContent::UploadSuccess(blob_hash) => blob_hash,
        _ => anyhow::bail!("Expected UploadSuccess but got {:?}", event.content)
    };

    blob_storage.download(&blob_hash);

    let event = events.recv().expect("receive an event for download");
    let bytes = match event.content {
        EventContent::DownloadSuccess(bytes) => bytes,
        _ => anyhow::bail!("Expected DownloadSuccess but got {:?}", event.content)
    };

    assert_eq!(dummy_payload, bytes);

    Ok(())
}

fn make_directory_with_stuff() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().expect("create tempdir for local blob storage");
    let encrypt = EncryptWithChacha::new_with_key_from_file(make_dummy_keyfile().path()).expect("create encrypt");
    let mut file = std::fs::File::create(tempdir.path().join("a_file")).expect("create a file in tempdir");
    let plain_text = bytes::Bytes::from("Hello world");
    let blob = encrypt.encrypt_blob(plain_text.clone()).expect("encrypt blob");
    file.write_all(blob.as_ref()).expect("fill file with stuff");
    tempdir
}

#[test]
fn download_blocking_twice() -> Result<()> {

    let tempdir = make_directory_with_stuff();
    let mut blob_storage = make_dummy_blob_storage(tempdir.path());
    blob_storage.download_blocking("a_file")?;
    blob_storage.download_blocking("a_file")?;

    Ok(())
}