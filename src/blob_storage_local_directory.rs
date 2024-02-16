use super::thread_sync::{Sender, Receiver};
use std::path::{Path, PathBuf};
use bytes::Bytes;
use log::debug;
use super::blob_storage::*;
use super::blob_encryption::EncryptWithChacha;
use anyhow::Context;

pub struct BlobStorageLocalDirectory {
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

struct AsyncComm {
    senders: Vec<Sender<Event>>,
    task_id: TaskId
}

struct SyncComm<'a> {
    events: &'a mut Vec<Event>
}

trait Comm {
    fn send_event(&mut self, event: &Event);
    fn task_id(&self) -> TaskId;

    fn send_error_event(&mut self, err_msg: String) {
        debug!("Error in task {}: {}", self.task_id().to_u64(), err_msg);
        let event = Event { id: self.task_id(), content: EventContent::Error(Error { msg: err_msg })};
        self.send_event(&event);
    }

    fn send_upload_success_event(&mut self, key: String) {
        debug!("Success in task {}", self.task_id().to_u64());
        let event = Event { id: self.task_id(), content: EventContent::UploadSuccess(key)};
        self.send_event(&event);
    }
}

impl Comm for AsyncComm {
    fn send_event(&mut self, event: &Event) {
        for sender in &self.senders {
            // it's ok if it's disconnected
            let _ = sender.send(event.clone());
        }
    }
    fn task_id(&self) -> TaskId {
        self.task_id
    }
}

impl<'a> Comm for SyncComm<'a> {
    fn send_event(&mut self, event: &Event) {
        self.events.push(event.clone());
    }
    fn task_id(&self) -> TaskId {
        TaskId::from_u64(0)
    }
}

fn set_thread_panic_hook() {
    use std::{panic::{set_hook, take_hook}, process::exit};
    let orig_hook = take_hook();
    set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        exit(1);
    }));
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
                comm.send_upload_success_event(key);
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
        let event = Event { id: comm.task_id(), content};
        comm.send_event(&event);
    }
}

impl Task for ExistsTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        let path_exists = self.blob_path.exists();
        let content = EventContent::ExistsSuccess(path_exists);
        let event = Event { id: comm.task_id(), content};
        comm.send_event(&event);
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
            encrypt,
            task_helper: TaskHelper::new()
        };
        Ok(me)
    }
}

impl BlobStorage for BlobStorageLocalDirectory {
    fn upload(&mut self, data: Bytes, key: Option<&str>) -> TaskId {
        let task = self.new_upload_task(data, key);
        self.task_helper.run_task(task)
    }

    fn download(&mut self, key: &str) -> TaskId {
        let task = self.new_download_task(key);
        self.task_helper.run_task(task)
    }

    fn exists(&mut self, key: &str) -> TaskId {
        let task = self.new_exists_task(key);
        self.task_helper.run_task(task)
    }

    fn events(&mut self) -> Receiver<Event> {
        let (sender, receiver) = super::thread_sync::channel::<Event>();
        self.task_helper.senders.push(sender);
        receiver
    }

    fn upload_blocking(&mut self, data: Bytes, key: Option<&str>) -> UploadResult {

        let mut task = self.new_upload_task(data, key);

        let mut events = Vec::new();
        task.run(SyncComm { events: &mut events });

        for event in &events {
            match &event.content {
                EventContent::UploadSuccess(result) => return Ok(result.clone()),
                EventContent::Error(err) => return Err(err.clone()),
                _ => todo!()
            };
        }

        panic!("Did not find event");
    }

    fn download_blocking(&mut self, key: &str) -> DownloadResult {

        let mut task = self.new_download_task(key);

        let mut events = Vec::new();
        task.run(SyncComm { events: &mut events });

        for event in &events {
            match &event.content {
                EventContent::DownloadSuccess(result) => return Ok(result.clone()),
                EventContent::Error(err) => return Err(err.clone()),
                _ => todo!()
            };
        }

        panic!("Did not find event");
    }

    fn exists_blocking(&mut self, key: &str) -> ExistsResult {

        let mut task = self.new_exists_task(key);

        let mut events = Vec::new();
        task.run(SyncComm { events: &mut events });

        for event in &events {
            match &event.content {
                EventContent::ExistsSuccess(result) => return Ok(*result),
                EventContent::Error(err) => return Err(err.clone()),
                _ => todo!()
            };
        }

        panic!("Did not find event");
    }
}

impl BlobStorageLocalDirectory {
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

struct TaskHelper {
    senders: Vec<Sender<Event>>,
    next_task_id: u64,
}

trait Task : Send {
    fn run<T: Comm>(&mut self, comm: T);
}

impl TaskHelper {
    fn new() -> Self {
        Self {
            senders: Vec::new(),
            next_task_id: 0,
        }
    }

    fn run_task<T: Task + 'static>(&mut self, mut task: T) -> TaskId {
        let task_id = TaskId::from_u64(self.next_task_id);
        self.next_task_id += 1;

        self.clean_senders();

        let senders = self.senders.clone();

        std::thread::spawn(move || {
            set_thread_panic_hook();
            task.run(AsyncComm { senders, task_id });
        });

        task_id
    }

    fn clean_senders(&mut self) {
        let num_senders_before = self.senders.len();
        self.senders.retain(|sender| !sender.disconnected());
        let num_sender_diff = num_senders_before - self.senders.len();
        if num_sender_diff > 0 {
            debug!("Removed {} senders", num_sender_diff);
        }
    }
}