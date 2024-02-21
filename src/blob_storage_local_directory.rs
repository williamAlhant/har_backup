use std::path::{Path, PathBuf};
use bytes::Bytes;
use log::debug;
use anyhow::Context;
use super::blob_storage::{
    self, Event, EventContent, get_hash_name, BlobStorage};
use super::blob_encryption::EncryptWithChacha;
use super::blob_storage_tasks::{
    Comm, Task, TaskHelper, TaskProvider};
use delegate::delegate;

struct BlobStorageLocalDirectoryImpl {
    local_dir_path: PathBuf,
    encrypt: EncryptWithChacha,
    task_helper: TaskHelper
}

struct UploadTask {
    local_dir_path: PathBuf,
    key: Option<String>,
    data: Bytes,
    encrypt: EncryptWithChacha
}

struct DownloadTask {
    blob_path: PathBuf,
    encrypt: EncryptWithChacha
}

struct ExistsTask {
    blob_path: PathBuf,
}

impl Task for UploadTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        debug!("Running UploadTask id:{}", comm.task_id().to_u64());

        let key = match &self.key {
            Some(key) => key.clone(),
            None => get_hash_name(self.local_dir_path.to_str().unwrap(), self.data.clone())
        };
        let path = self.local_dir_path.join(key.as_str());

        let data = match self.encrypt.encrypt_blob(self.data.clone()) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while encrypting ({})", err);
                comm.send_error_event(err_msg);
                return;
            }
        };

        match std::fs::write(path, data.as_ref()) {
            Ok(_) => {
                comm.send_event_content(EventContent::UploadSuccess(key));
            },
            Err(err) => {
                let err_msg = format!("Error while opening file ({})", err);
                comm.send_error_event(err_msg);
            }
        };
    }
}

impl Task for DownloadTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        debug!("Running DownloadTask id:{}", comm.task_id().to_u64());

        let blob = match std::fs::read(&self.blob_path) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while opening/reading {:?} ({})", self.blob_path.to_str(), err);
                comm.send_error_event(err_msg);
                return;
            }
        };

        let decrypted = match self.encrypt.decrypt_blob(bytes::Bytes::from(blob)) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while decrypting ({})", err);
                comm.send_error_event(err_msg);
                return;
            }
        };

        debug!("Success in task {}", comm.task_id().to_u64());
        let content = EventContent::DownloadSuccess(decrypted);
        comm.send_event_content(content);
    }
}

impl Task for ExistsTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        let path_exists = self.blob_path.exists();
        let content = EventContent::ExistsSuccess(path_exists);
        comm.send_event_content(content);
    }
}

impl BlobStorageLocalDirectoryImpl {
    pub fn new(local_dir_path: &Path, encryption_key_file: &Path) -> anyhow::Result<Self> {
        if !local_dir_path.exists() {
            anyhow::bail!("BlobStorageLocalDirectory::new Directory does not exist")
        }
        let encrypt = EncryptWithChacha::new_with_key_from_file(encryption_key_file).context("Opening key file")?;
        let me = Self {
            local_dir_path: local_dir_path.to_path_buf(),
            encrypt,
            task_helper: TaskHelper::new()
        };
        Ok(me)
    }
}

impl TaskProvider for BlobStorageLocalDirectoryImpl {

    type UploadTask = UploadTask;
    type DownloadTask = DownloadTask;
    type ExistsTask = ExistsTask;

    fn task_helper(&mut self) -> &mut TaskHelper {
        &mut self.task_helper
    }

    fn new_upload_task(&self, data: Bytes, key: Option<&str>) -> UploadTask {
        UploadTask {
            local_dir_path: self.local_dir_path.clone(),
            key: key.map(String::from),
            data,
            encrypt: self.encrypt.clone()
        }
    }

    fn new_download_task(&self, key: &str) -> DownloadTask {
        DownloadTask {
            blob_path: self.local_dir_path.join(key),
            encrypt: self.encrypt.clone()
        }
    }

    fn new_exists_task(&self, key: &str) -> ExistsTask {
        ExistsTask {
            blob_path: self.local_dir_path.join(key),
        }
    }
}

pub struct BlobStorageLocalDirectory {
    inner: BlobStorageLocalDirectoryImpl
}

impl BlobStorageLocalDirectory {
    pub fn new(local_dir_path: &Path, encryption_key_file: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            inner: BlobStorageLocalDirectoryImpl::new(local_dir_path, encryption_key_file)?
        })
    }
}

impl BlobStorage for BlobStorageLocalDirectory {
    delegate! {
        to self.inner {
            fn upload(&mut self, data: Bytes, key: Option<&str>) -> blob_storage::TaskId;
            fn download(&mut self, key: &str) -> blob_storage::TaskId;
            fn exists(&mut self, key: &str) -> blob_storage::TaskId;
            fn events(&mut self) -> crate::thread_sync::Receiver<Event>;

            fn upload_blocking(&mut self, data: Bytes, key: Option<&str>) -> blob_storage::UploadResult;
            fn download_blocking(&mut self, key: &str) -> blob_storage::DownloadResult;
            fn exists_blocking(&mut self, key: &str) -> blob_storage::ExistsResult;
        }
    }
}