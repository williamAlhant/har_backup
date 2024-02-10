use std::io::Read;
use std::sync::mpsc::Receiver;

#[derive(Debug, Clone, Copy)]
pub struct UploadId {
    id: u64
}

impl UploadId {
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
    pub id: UploadId
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[task:{}] {:?}", self.id.id, self.content)
    }
}

#[derive(Debug, Clone)]
pub enum EventContent {
    Success(),
    Error(Error),
    Progress(Progress)
}

pub trait BlobStorage {
    fn upload<R: Read + Send + 'static>(&mut self, data: R) -> UploadId;
    fn events(&mut self) -> Receiver<Event>;
}
