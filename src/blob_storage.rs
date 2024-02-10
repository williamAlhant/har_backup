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
    transmitted_bytes: u64
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
    UploadSuccess,
    DownloadSuccess(Bytes),
    Error(Error),
    Progress(Progress)
}

impl std::fmt::Debug for EventContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventContent::UploadSuccess => write!(f, "UploadSuccess"),
            EventContent::DownloadSuccess(inner) => write!(f, "DownloadSuccess(...)"),
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
