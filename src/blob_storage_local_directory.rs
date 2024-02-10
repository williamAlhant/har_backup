use std::sync::mpsc::{Sender, Receiver};
use std::path::{Path, PathBuf};
use bytes::Bytes;
use log::debug;
use super::blob_storage::*;
use super::blob_encryption::EncryptWithChacha;

pub struct BlobStorageLocalDirectory {
    local_dir_path: PathBuf,
    comm: Vec<Sender<Event>>,
    next_upload_id: u64,
    encrypt: EncryptWithChacha
}

struct Task {
    local_dir_path: PathBuf,
    comm: Vec<Sender<Event>>,
    id: UploadId,
    data: Bytes,
    encrypt: EncryptWithChacha
}

impl Task {
    fn do_task(&mut self) {
        let hash = blake3::hash(self.data.as_ref());
        let hash_hex = hash.to_hex();

        let path = self.local_dir_path.join(hash_hex.as_str());

        let data = match self.encrypt.encrypt_blob(self.data.clone()) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while encrypting ({})", err);
                let event = self.make_error_event(err_msg);
                self.send_event(&event);
                return;
            }
        };

        match std::fs::write(path, data.as_ref()) {
            Ok(_) => {
                let event = self.make_success_event();
                self.send_event(&event);
            },
            Err(err) => {
                let err_msg = format!("Error while opening file ({})", err);
                let event = self.make_error_event(err_msg);
                self.send_event(&event);
            }
        };
    }

    fn send_event(&mut self, event: &Event) {
        for sender in &self.comm {
            sender.send(event.clone()).expect("No receiver on the other end, is it ok?");
        }
    }

    fn make_error_event(&self, err_msg: String) -> Event {
        debug!("Error in task {}: {}", self.id.to_u64(), err_msg);
        Event { id: self.id, content: EventContent::Error(Error { msg: err_msg })}
    }

    fn make_success_event(&self) -> Event {
        debug!("Success in task {}", self.id.to_u64());
        Event { id: self.id, content: EventContent::Success()}
    }
}

impl BlobStorageLocalDirectory {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            anyhow::bail!("BlobStorageLocalDirectory::new Directory does not exist")
        }
        let dummy_key_path = PathBuf::from("/home/cookie/code/har_backup/test_files/keyfile");
        let encrypt = EncryptWithChacha::new_with_key_from_file(&dummy_key_path)?;
        let me = Self {
            local_dir_path: path.to_path_buf(),
            comm: Vec::new(),
            next_upload_id: 0,
            encrypt
        };
        Ok(me)
    }
}

impl BlobStorage for BlobStorageLocalDirectory {
    fn upload(&mut self, data: Bytes) -> UploadId {
        let upload_id = UploadId::from_u64(self.next_upload_id);
        self.next_upload_id += 1;

        let mut task = Task {
            local_dir_path: self.local_dir_path.clone(),
            comm: self.comm.clone(),
            id: upload_id,
            data,
            encrypt: self.encrypt.clone()
        };

        debug!("Spawning upload task for id {}", upload_id.to_u64());

        std::thread::spawn(move || {
            task.do_task();
        });

        upload_id
    }

    fn events(&mut self) -> Receiver<Event> {
        let (sender, receiver) = std::sync::mpsc::channel::<Event>();
        self.comm.push(sender);
        receiver
    }
}