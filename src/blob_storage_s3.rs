use crate::blob_storage::{BlobStorage, Event, EventContent};
use crate::blob_storage_tasks::{Comm, Task, TaskHelper, implement_blob_storage_for_task_provider};

pub struct BlobStorageS3 {
    task_helper: TaskHelper,
}

struct UploadTask {}
struct DownloadTask {}
struct ExistsTask {}

impl Task for UploadTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        todo!();
    }
}

impl Task for DownloadTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        todo!();
    }
}

impl Task for ExistsTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        todo!();
    }
}

impl BlobStorageS3 {
    fn new_upload_task(&self, data: bytes::Bytes, key: Option<&str>) -> UploadTask {
        todo!();
    }

    fn new_download_task(&self, key: &str) -> DownloadTask {
        todo!();
    }

    fn new_exists_task(&self, key: &str) -> ExistsTask {
        todo!();
    }
}

implement_blob_storage_for_task_provider!(BlobStorageS3);
