use super::thread_sync::Receiver;
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TaskId {
    id: u64
}

impl TaskId {
    pub fn to_u64(&self) -> u64 {
        self.id
    }

    pub fn from_u64(val: u64) -> Self {
        Self { id: val }
    }
}

#[derive(Debug, Clone)]
pub struct Error {
    pub msg: String
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for Error {

}

#[derive(Debug, Clone)]
pub struct Progress {
    // transmitted_bytes: u64
}

#[derive(Debug, Clone)]
pub struct Event {
    pub content: EventContent,
    pub id: TaskId
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[task:{}] {:?}", self.id.id, self.content)
    }
}

#[derive(Clone)]
pub enum EventContent {
    UploadSuccess(String), // contains blob name/key, ie hash of encrypted data
    DownloadSuccess(Bytes), // contains downloaded data
    Error(Error),
    Progress(Progress)
}

pub type UploadResult = Result<String, Error>;
pub type DownloadResult = Result<Bytes, Error>;

impl std::fmt::Debug for EventContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventContent::UploadSuccess(inner) => write!(f, "UploadSuccess({:?})", inner),
            EventContent::DownloadSuccess(_) => write!(f, "DownloadSuccess(...)"),
            EventContent::Error(inner) => write!(f, "Error({:?})", inner),
            EventContent::Progress(inner) => write!(f, "Progress({:?})", inner),
        }
    }
}

pub trait BlobStorage {
    fn upload(&mut self, data: Bytes, key: Option<&str>) -> TaskId;
    fn download(&mut self, key: &str) -> TaskId;
    fn events(&mut self) -> Receiver<Event>;

    fn upload_blocking(&mut self, data: Bytes, key: Option<&str>) -> UploadResult {
        let events = self.events();
        let task_id = self.upload(data, key);
        // todo, loop until the taskId matches the event...
        let event = events.recv().expect("receive an event for upload");
        assert!(event.id == task_id);
        match event.content {
            EventContent::UploadSuccess(key) => Ok(key),
            EventContent::Error(err) => Err(err),
            _ => todo!()
        }
    }

    fn download_blocking(&mut self, key: &str) -> DownloadResult {
        let events = self.events();
        let task_id = self.download(key);
        // todo, loop until the taskId matches the event...
        let event = events.recv().expect("receive an event for download");
        assert!(event.id == task_id);
        match event.content {
            EventContent::DownloadSuccess(bytes) => Ok(bytes),
            EventContent::Error(err) => Err(err),
            _ => todo!()
        }
    }
}

pub(crate) fn get_hash_name(bucket_name: &str, data: Bytes) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update("har_backup".as_bytes());
    hasher.update(bucket_name.as_bytes());
    hasher.update(data.as_ref());
    let hash = hasher.finalize();
    let hash_hex = hash.to_hex();
    hash_hex.to_string()
}