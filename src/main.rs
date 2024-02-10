use std::io::Read;
use std::sync::mpsc::{Sender, Receiver};
use std::path::{Path, PathBuf};
use anyhow::Context;
use log::debug;

#[derive(Debug, Clone, Copy)]
struct BlobStorageUploadId {
    id: u64
}

#[derive(Debug, Clone)]
struct BlobStorageError {
    msg: String
}

#[derive(Debug, Clone)]
struct BlobStorageProgress {
    transmitted_bytes: u64
}

#[derive(Debug, Clone)]
struct BlobStorageEvent {
    content: BlobStorageEventContent,
    id: BlobStorageUploadId
}

impl std::fmt::Display for BlobStorageEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[task:{}] {:?}", self.id.id, self.content)
    }
}

#[derive(Debug, Clone)]
enum BlobStorageEventContent {
    Success(),
    Error(BlobStorageError),
    Progress(BlobStorageProgress)
}

trait BlobStorage {
    fn upload<R: Read + Send + 'static>(&mut self, data: R) -> BlobStorageUploadId;
    fn events(&mut self) -> Receiver<BlobStorageEvent>;
}

struct BlobStorageLocalDirectory {
    local_dir_path: PathBuf,
    comm: Vec<Sender<BlobStorageEvent>>,
    next_upload_id: u64
}

struct BlobStorageLocalDirectoryTask<R: Read> {
    local_dir_path: PathBuf,
    comm: Vec<Sender<BlobStorageEvent>>,
    id: BlobStorageUploadId,
    data: R
}

impl<R: Read> BlobStorageLocalDirectoryTask<R> {
    fn do_task(&mut self) {
        let filesink = std::fs::File::open(self.local_dir_path.join("dummy"));
        if filesink.is_err() {
            let err_msg = format!("Error while opening file ({})", filesink.err().unwrap());
            let event = self.make_error_event(err_msg);
            self.send_event(&event);
            return;
        }
    }

    fn send_event(&mut self, event: &BlobStorageEvent) {
        for sender in &self.comm {
            sender.send(event.clone()).expect("No receiver on the other end, is it ok?");
        }
    }

    fn make_error_event(&self, err_msg: String) -> BlobStorageEvent {
        debug!("Error in task {}: {}", self.id.id, err_msg);
        BlobStorageEvent { id: self.id, content: BlobStorageEventContent::Error(BlobStorageError { msg: err_msg })}
    }
}

impl BlobStorageLocalDirectory {
    fn new(path: &Path) -> anyhow::Result<Self> {
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
    fn upload<R: Read + Send + 'static>(&mut self, data: R) -> BlobStorageUploadId {
        let upload_id = BlobStorageUploadId { id: self.next_upload_id };
        self.next_upload_id += 1;

        let mut task = BlobStorageLocalDirectoryTask {
            local_dir_path: self.local_dir_path.clone(),
            comm: self.comm.clone(),
            id: upload_id,
            data
        };

        debug!("Spawning upload task for id {}", upload_id.id);

        std::thread::spawn(move || {
            task.do_task();
        });

        upload_id
    }

    fn events(&mut self) -> Receiver<BlobStorageEvent> {
        let (sender, receiver) = std::sync::mpsc::channel::<BlobStorageEvent>();
        self.comm.push(sender);
        receiver
    }
}

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
