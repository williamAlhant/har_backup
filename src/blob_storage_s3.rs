use crate::blob_storage::{BlobStorage, Event, EventContent};
use crate::blob_storage_tasks::{Comm, Task, TaskHelper, implement_blob_storage_for_task_provider};
use rusty_s3::{Bucket, Credentials, UrlStyle};
use anyhow::Context;
use log::debug;

pub struct BlobStorageS3 {
    task_helper: TaskHelper,
    bucket: Bucket,
    credentials: Credentials,
}

impl BlobStorageS3 {
    pub fn new(endpoint: &str, bucket: &str, key: &str, secret: &str) -> anyhow::Result<Self> {
        let endpoint = endpoint.parse().context("parsing endpoint")?;
        let bucket = bucket.to_string();
        let bucket = Bucket::new(endpoint, UrlStyle::VirtualHost, bucket, "toto").expect("Create rusty_s3 bucket");
        debug!("Init s3 bucket: {:?}", bucket);
        let credentials = Credentials::new(key, secret);
        Ok(Self {
            task_helper: TaskHelper::new(),
            bucket,
            credentials,
        })
    }
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
