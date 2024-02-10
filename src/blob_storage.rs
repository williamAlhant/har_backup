use std::io::Read;
use std::sync::mpsc::Receiver;

#[derive(Debug, Clone, Copy)]
pub struct BlobStorageUploadId {
    id: u64
}

impl BlobStorageUploadId {
    pub fn to_u64(&self) -> u64 {
        self.id
    }

    pub fn from_u64(val: u64) -> Self {
        Self { id: val }
    }
}

#[derive(Debug, Clone)]
pub struct BlobStorageError {
    pub msg: String
}

#[derive(Debug, Clone)]
pub struct BlobStorageProgress {
    transmitted_bytes: u64
}

#[derive(Debug, Clone)]
pub struct BlobStorageEvent {
    pub content: BlobStorageEventContent,
    pub id: BlobStorageUploadId
}

impl std::fmt::Display for BlobStorageEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[task:{}] {:?}", self.id.id, self.content)
    }
}

#[derive(Debug, Clone)]
pub enum BlobStorageEventContent {
    Success(),
    Error(BlobStorageError),
    Progress(BlobStorageProgress)
}

pub trait BlobStorage {
    fn upload<R: Read + Send + 'static>(&mut self, data: R) -> BlobStorageUploadId;
    fn events(&mut self) -> Receiver<BlobStorageEvent>;
}
