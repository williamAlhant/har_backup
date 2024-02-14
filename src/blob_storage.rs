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
    Progress(Progress),
    ExistsSuccess(bool),
}

pub type UploadResult = Result<String, Error>;
pub type DownloadResult = Result<Bytes, Error>;
pub type ExistsResult = Result<bool, Error>;

impl std::fmt::Debug for EventContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventContent::DownloadSuccess(_) => write!(f, "DownloadSuccess(...)"),
            EventContent::UploadSuccess(a) => write!(f, "UploadSuccess({:?})", a),
            EventContent::Error(a) => write!(f, "Error({:?})", a),
            EventContent::Progress(a) => write!(f, "Progress({:?})", a),
            EventContent::ExistsSuccess(a) => write!(f, "ExistsSuccess({:?})", a),
        }
    }
}

pub trait BlobStorage {
    fn upload(&mut self, data: Bytes, key: Option<&str>) -> TaskId;
    fn download(&mut self, key: &str) -> TaskId;
    fn exists(&mut self, key: &str) -> TaskId;
    fn events(&mut self) -> Receiver<Event>;

    fn upload_blocking(&mut self, data: Bytes, key: Option<&str>) -> UploadResult;
    fn download_blocking(&mut self, key: &str) -> DownloadResult;
    fn exists_blocking(&mut self, key: &str) -> ExistsResult;
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

#[cfg(test)]
mod tests {
    use super::EventContent;

    #[test]
    fn print_debug_event_content() {
        let event = EventContent::ExistsSuccess(false);
        println!("{:?}", event);
    }
}