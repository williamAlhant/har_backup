use crate::blob_storage::{self, BlobStorage, Event, EventContent, get_hash_name};
use crate::blob_storage_tasks::{Comm, Task, TaskHelper, TaskProvider};
use crate::blob_encryption::EncryptWithChacha;
use std::path::Path;
use std::io::Read;
use rusty_s3::{Bucket, Credentials, UrlStyle, S3Action};
use url::Url;
use bytes::Bytes;
use anyhow::Context;
use log::debug;
use delegate::delegate;

const PRESIGNED_URL_DURATION: std::time::Duration = std::time::Duration::from_secs(60 * 60);

struct BlobStorageS3Impl {
    task_helper: TaskHelper,
    bucket: Bucket,
    credentials: Credentials,
    encrypt: EncryptWithChacha,
}

impl BlobStorageS3Impl {
    pub fn new(endpoint: &str, bucket: &str, key: &str, secret: &str, encryption_key_file: &Path) -> anyhow::Result<Self> {
        let endpoint = endpoint.parse().context("parsing endpoint")?;
        let bucket = bucket.to_string();
        let bucket = Bucket::new(endpoint, UrlStyle::VirtualHost, bucket, "toto").expect("Create rusty_s3 bucket");
        debug!("Init s3 bucket: {:?}", bucket);
        let credentials = Credentials::new(key, secret);
        let encrypt = EncryptWithChacha::new_with_key_from_file(encryption_key_file).context("Opening key file")?;
        Ok(Self {
            task_helper: TaskHelper::new(),
            bucket,
            credentials,
            encrypt,
        })
    }
}

struct UploadTask {
    bucket: Bucket,
    credentials: Credentials,
    key: Option<String>,
    data: Bytes,
    encrypt: EncryptWithChacha,
}

struct DownloadTask {
    url: Url,
    encrypt: EncryptWithChacha,
}

struct ExistsTask {
    url: Url,
}

impl Task for UploadTask {
    fn run<T: Comm>(&mut self, mut comm: T) {

        let key = match &self.key {
            Some(key) => key.clone(),
            None => get_hash_name(self.bucket.name(), self.data.clone())
        };

        let data = match self.encrypt.encrypt_blob(self.data.clone()) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while encrypting ({})", err);
                comm.send_error_event(err_msg);
                return;
            }
        };

        let action = self.bucket.put_object(Some(&self.credentials), key.as_str());
        let url = action.sign(PRESIGNED_URL_DURATION);
        let response = ureq::request_url("PUT", &url).send_bytes(data.as_ref());
        match response {
            Err(err) => {
                let err_msg = format!("Error while uploading ({})", err);
                comm.send_error_event(err_msg);
                return;
            },
            Ok(_) => (),
        };

        comm.send_event_content(EventContent::UploadSuccess(key));
    }
}

impl Task for DownloadTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        let response = ureq::request_url("GET", &self.url).call();
        let response = match response {
            Err(err) => {
                let err_msg = format!("Error while downloading ({})", err);
                comm.send_error_event(err_msg);
                return;
            },
            Ok(v) => v,
        };

        let mut buf = Vec::new();
        match response.into_reader().read_to_end(&mut buf) {
            Ok(_) => (),
            Err(err) => {
                let err_msg = format!("Error while reading response content ({})", err);
                comm.send_error_event(err_msg);
                return;
            },
        };
        let blob = Bytes::from(buf);

        let decrypted = match self.encrypt.decrypt_blob(bytes::Bytes::from(blob)) {
            Ok(data) => data,
            Err(err) => {
                let err_msg = format!("Error while decrypting ({})", err);
                comm.send_error_event(err_msg);
                return;
            }
        };

        debug!("Success in task {}", comm.task_id().to_u64());
        let content = EventContent::DownloadSuccess(decrypted);
        comm.send_event_content(content);
    }
}

impl Task for ExistsTask {
    fn run<T: Comm>(&mut self, mut comm: T) {
        let response = ureq::request_url("HEAD", &self.url).call();
        match response {
            Err(err) => {
                match err {
                    ureq::Error::Status(code, _) => {
                        if code == 404 {
                            let content = EventContent::ExistsSuccess(false);
                            comm.send_event_content(content);
                        }
                        else {
                            let err_msg = format!("Error while head'ing ({})", err);
                            comm.send_error_event(err_msg);
                        }
                    },
                    ureq::Error::Transport(err) => {
                        let err_msg = format!("Error while head'ing ({})", err);
                        comm.send_error_event(err_msg);
                    },
                };
                return;
            },
            Ok(_) => {
                let content = EventContent::ExistsSuccess(true);
                comm.send_event_content(content);
            },
        };
    }
}

impl TaskProvider for BlobStorageS3Impl {

    type UploadTask = UploadTask;
    type DownloadTask = DownloadTask;
    type ExistsTask = ExistsTask;

    fn task_helper(&mut self) -> &mut TaskHelper {
        &mut self.task_helper
    }

    fn new_upload_task(&self, data: bytes::Bytes, key: Option<&str>) -> UploadTask {
        UploadTask {
            bucket: self.bucket.clone(),
            credentials: self.credentials.clone(),
            data,
            encrypt: self.encrypt.clone(),
            key: key.map(String::from),
        }
    }

    fn new_download_task(&self, key: &str) -> DownloadTask {
        let action = self.bucket.get_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);
        DownloadTask {
            url,
            encrypt: self.encrypt.clone(),
        }
    }

    fn new_exists_task(&self, key: &str) -> ExistsTask {
        let action = self.bucket.head_object(Some(&self.credentials), key);
        let url = action.sign(PRESIGNED_URL_DURATION);
        ExistsTask {
            url,
        }
    }
}

pub struct BlobStorageS3 {
    inner: BlobStorageS3Impl
}

impl BlobStorageS3 {
    pub fn new(endpoint: &str, bucket: &str, key: &str, secret: &str, encryption_key_file: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            inner: BlobStorageS3Impl::new(endpoint, bucket, key, secret, encryption_key_file)?
        })
    }
}

impl BlobStorage for BlobStorageS3 {
    delegate! {
        to self.inner {
            fn upload(&mut self, data: Bytes, key: Option<&str>) -> blob_storage::TaskId;
            fn download(&mut self, key: &str) -> blob_storage::TaskId;
            fn exists(&mut self, key: &str) -> blob_storage::TaskId;
            fn events(&mut self) -> crate::thread_sync::Receiver<Event>;

            fn upload_blocking(&mut self, data: Bytes, key: Option<&str>) -> blob_storage::UploadResult;
            fn download_blocking(&mut self, key: &str) -> blob_storage::DownloadResult;
            fn exists_blocking(&mut self, key: &str) -> blob_storage::ExistsResult;
        }
    }
}