use std::sync::mpsc::{Sender, Receiver};
use std::path::{Path, PathBuf};
use bytes::Bytes;
use log::debug;
use super::blob_storage::*;
use super::blob_encryption::EncryptWithChacha;
use anyhow::Context;

pub struct BlobStorageLocalDirectory {
    local_dir_path: PathBuf,
    senders: Vec<Sender<Event>>,
    next_task_id: u64,
    encrypt: EncryptWithChacha
}

struct UploadTask {
    local_dir_path: PathBuf,
    comm: Comm,
    data: Bytes,
    encrypt: EncryptWithChacha
}

struct DownloadTask {
    blob_path: PathBuf,
    comm: Comm,
    encrypt: EncryptWithChacha
}

struct Comm {
    senders: Vec<Sender<Event>>,
    task_id: TaskId
}

impl Comm {
    fn send_event(&mut self, event: &Event) {
        for sender in &self.senders {
            sender.send(event.clone()).expect("No receiver on the other end, is it ok?");
        }
    }

    fn send_error_event(&mut self, err_msg: String) {
        debug!("Error in task {}: {}", self.task_id.to_u64(), err_msg);
        let event = Event { id: self.task_id, content: EventContent::Error(Error { msg: err_msg })};
        self.send_event(&event);
    }

    fn send_upload_success_event(&mut self, key: String) {
        debug!("Success in task {}", self.task_id.to_u64());
        let event = Event { id: self.task_id, content: EventContent::UploadSuccess(key)};
        self.send_event(&event);
    }
}

impl UploadTask {
    fn do_task(&mut self) {

        let hash_name = get_hash_name(self.local_dir_path.to_str().unwrap(), self.data.clone());
        let path = self.local_dir_path.join(hash_name.as_str());

        let data = match self.encrypt.encrypt_blob(self.data.clone()) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while encrypting ({})", err);
                self.comm.send_error_event(err_msg);
                return;
            }
        };

        match std::fs::write(path, data.as_ref()) {
            Ok(_) => {
                self.comm.send_upload_success_event(hash_name);
            },
            Err(err) => {
                let err_msg = format!("Error while opening file ({})", err);
                self.comm.send_error_event(err_msg);
            }
        };
    }
}

impl DownloadTask {
    fn do_task(&mut self) {
        let blob = match std::fs::read(&self.blob_path) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while opening/reading {:?} ({})", self.blob_path.to_str(), err);
                self.comm.send_error_event(err_msg);
                return;
            }
        };

        let decrypted = match self.encrypt.decrypt_blob(bytes::Bytes::from(blob)) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while decrypting ({})", err);
                self.comm.send_error_event(err_msg);
                return;
            }
        };

        debug!("Success in task {}", self.comm.task_id.to_u64());
        let content = EventContent::DownloadSuccess(decrypted);
        let event = Event { id: self.comm.task_id, content};
        self.comm.send_event(&event);
    }
}

impl BlobStorageLocalDirectory {
    pub fn new(local_dir_path: &Path, encryption_key_file: &Path) -> anyhow::Result<Self> {
        if !local_dir_path.exists() {
            anyhow::bail!("BlobStorageLocalDirectory::new Directory does not exist")
        }
        let encrypt = EncryptWithChacha::new_with_key_from_file(encryption_key_file).context("Opening key file")?;
        let me = Self {
            local_dir_path: local_dir_path.to_path_buf(),
            senders: Vec::new(),
            next_task_id: 0,
            encrypt
        };
        Ok(me)
    }
}

impl BlobStorage for BlobStorageLocalDirectory {
    fn upload(&mut self, data: Bytes) -> TaskId {
        let upload_id = TaskId::from_u64(self.next_task_id);
        self.next_task_id += 1;

        let mut task = UploadTask {
            local_dir_path: self.local_dir_path.clone(),
            comm: Comm { senders: self.senders.clone(), task_id: upload_id },
            data,
            encrypt: self.encrypt.clone()
        };

        debug!("Spawning upload task for id {}", upload_id.to_u64());

        std::thread::spawn(move || {
            task.do_task();
        });

        upload_id
    }

    fn download(&mut self, key: &str) -> TaskId {
        let download_id = TaskId::from_u64(self.next_task_id);
        self.next_task_id += 1;

        let mut task = DownloadTask {
            blob_path: self.local_dir_path.join(key),
            comm: Comm { senders: self.senders.clone(), task_id: download_id },
            encrypt: self.encrypt.clone()
        };

        debug!("Spawning download task for id {}", download_id.to_u64());

        std::thread::spawn(move || {
            task.do_task();
        });

        download_id
    }

    fn events(&mut self) -> Receiver<Event> {
        let (sender, receiver) = std::sync::mpsc::channel::<Event>();
        self.senders.push(sender);
        receiver
    }
}