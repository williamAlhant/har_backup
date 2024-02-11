use std::sync::mpsc::Receiver;
use bytes::Bytes;

#[derive(Debug, Clone, Copy)]
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
    fn upload(&mut self, data: Bytes) -> TaskId;
    fn download(&mut self, key: &str) -> TaskId;
    fn events(&mut self) -> Receiver<Event>;
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