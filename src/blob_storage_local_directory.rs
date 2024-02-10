use std::io::Read;
use std::sync::mpsc::{Sender, Receiver};
use std::path::{Path, PathBuf};
use log::debug;
use super::blob_storage::*;

pub struct BlobStorageLocalDirectory {
    local_dir_path: PathBuf,
    comm: Vec<Sender<Event>>,
    next_upload_id: u64
}

struct Task<R: Read> {
    local_dir_path: PathBuf,
    comm: Vec<Sender<Event>>,
    id: UploadId,
    data: R
}

impl<R: Read> Task<R> {
    fn do_task(&mut self) {
        let filesink = std::fs::File::open(self.local_dir_path.join("dummy"));
        if filesink.is_err() {
            let err_msg = format!("Error while opening file ({})", filesink.err().unwrap());
            let event = self.make_error_event(err_msg);
            self.send_event(&event);
            return;
        }
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
}

impl BlobStorageLocalDirectory {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            anyhow::bail!("BlobStorageLocalDirectory::new Directory does not exist")
        }
        let me = Self {
            local_dir_path: path.to_path_buf(),
            comm: Vec::new(),
            next_upload_id: 0
        };
        Ok(me)
    }
}

impl BlobStorage for BlobStorageLocalDirectory {
    fn upload<R: Read + Send + 'static>(&mut self, data: R) -> UploadId {
        let upload_id = UploadId::from_u64(self.next_upload_id);
        self.next_upload_id += 1;

        let mut task = Task {
            local_dir_path: self.local_dir_path.clone(),
            comm: self.comm.clone(),
            id: upload_id,
            data
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