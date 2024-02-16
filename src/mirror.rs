use crate::blob_storage::{self, BlobStorage};
use crate::manifest::Manifest;
use log::debug;
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::collections::HashMap;

pub struct Mirror {
    blob_storage: Box<dyn BlobStorage>
}

const MANIFEST_KEY: &str = "manifest";

impl Mirror {
    pub fn new(blob_storage: Box<dyn BlobStorage>) -> Self {
        Self {
            blob_storage
        }
    }

    // like git init; create/upload an empty remote manifest
    pub fn init(&mut self) -> anyhow::Result<()> {

        let exists = self.blob_storage.exists_blocking(MANIFEST_KEY)?;
        if exists {
            anyhow::bail!("Manifest already exists in remote");
        }

        let manifest = Manifest::new();
        let data = manifest.to_bytes()?;
        self.blob_storage.upload_blocking(data, Some(MANIFEST_KEY))?;
        Ok(())
    }

    pub fn get_manifest_blob(&mut self) -> Result<bytes::Bytes> {
        debug!("Download remote manifest...");
        let remote_manifest_bytes = self.blob_storage.download_blocking(MANIFEST_KEY)?;
        debug!("Download remote manifest done");
        Ok(remote_manifest_bytes)
    }

    pub fn push(&mut self, paths: &Vec<PathBuf>, prefix_path: &Path, config: PushConfig) -> Result<Vec<Option<blob_storage::UploadResult>>> {

        use blob_storage::{TaskId, EventContent, UploadResult};

        // map from taskid to result index
        let mut active_tasks: HashMap<TaskId, usize> = HashMap::new();
        let mut active_size = 0; // sum of size of files being transferred
        let mut results: Vec<Option<UploadResult>> = vec![None; paths.len()];
        let mut sizes: Vec<Option<usize>> = vec![None; paths.len()];
        let mut next_index = 0;
        let events = self.blob_storage.events();
        let mut time_of_last_print = std::time::Instant::now();
        let mut total_transferred = 0;

        while next_index < results.len() || active_tasks.len() > 0 {
            while next_index < results.len()
                    && (active_size < config.active_size_limit || active_tasks.is_empty())
                    && active_tasks.len() < config.active_tasks_limit {
                let file_path = prefix_path.join(&paths[next_index]);
                let data = std::fs::read(file_path)?;
                let data = bytes::Bytes::from(data);
                let data_size = data.len();
                let task_id = self.blob_storage.upload(data, None);
                active_tasks.insert(task_id, next_index);
                active_size += data_size;
                sizes[next_index] = Some(data_size);
                debug!("Started task {} for index {}", task_id.to_u64(), next_index);
                next_index += 1;
            }

            if active_tasks.len() > 0 {
                let event = events.recv()?;
                debug!("Got event {}", event);
                match event.content {
                    EventContent::Error(e) => anyhow::bail!(e),
                    EventContent::UploadSuccess(key) => {
                        let index = active_tasks[&event.id];
                        let result = UploadResult::Ok(key);
                        results[index] = Some(result);
                        let size = sizes[index].unwrap();
                        active_size -= size;
                        total_transferred += size;
                        active_tasks.remove(&event.id);
                    },
                    _ => panic!("Should not get anything except Error or UploadSuccess")
                }
            }

            let elapsed_since_last_print = std::time::Instant::now() - time_of_last_print;
            if elapsed_since_last_print > config.time_between_prints {
                let done_tasks = next_index; // not quite but good enough
                let total_tasks = results.len();
                let num_active = active_tasks.len();
                println!("Push status: {}/{} num active: {} transferred bytes: {} active tasks: {:?}", done_tasks, total_tasks, num_active, total_transferred, active_tasks.keys());
                time_of_last_print = std::time::Instant::now();
            }
        }

        Ok(results)
    }
}

pub struct PushConfig {
    active_tasks_limit: usize,
    active_size_limit: usize,
    time_between_prints: std::time::Duration,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            active_size_limit: 10_000_000,
            active_tasks_limit: 32,
            time_between_prints: std::time::Duration::from_millis(800),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use crate::blob_storage_local_directory::BlobStorageLocalDirectory;
    use std::io::Write;

    pub fn make_dummy_keyfile() -> NamedTempFile {
        let mut keyfile = NamedTempFile::new().expect("create tempfile for dummy encryption key");
        let key: [u8; 32] = [1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8];
        keyfile.write_all(&key).expect("write key file content");
        keyfile
    }
    
    pub fn make_dummy_blob_storage(dirpath: &Path) -> BlobStorageLocalDirectory {
        let keyfile = make_dummy_keyfile();
        BlobStorageLocalDirectory::new(dirpath, keyfile.path()).expect("create blob storage")
    }

    pub fn make_files(num_files: usize, file_size: usize) -> Vec<NamedTempFile> {
        let mut files = Vec::new();
        let big_data_buf: Vec<u8> = vec![42; file_size];
        for _ in 0..num_files {
            let mut file = NamedTempFile::new().expect("Create file to transfer");
            file.write_all(&big_data_buf).expect("Write file to transfer");
            files.push(file);
        }
        files
    }

    #[test]
    fn push() -> Result<()> {
        use std::time::Duration;

        let tempdir = tempfile::tempdir().expect("create tempdir for local blob storage");
        let blob_storage = make_dummy_blob_storage(tempdir.path());

        let mut mirror = Mirror::new(Box::new(blob_storage));
        let files = make_files(5, 1000);
        let paths: Vec<PathBuf> = files.iter().map(|f| PathBuf::from(f.path())).collect();

        let config = PushConfig { active_size_limit: 10_000_000, active_tasks_limit: 32, time_between_prints: Duration::from_millis(0) };
        mirror.push(&paths, Path::new(""), config)?;

        let config = PushConfig { active_size_limit: 100, active_tasks_limit: 32, time_between_prints: Duration::from_millis(0) };
        mirror.push(&paths, Path::new(""), config)?;

        Ok(())
    }
}