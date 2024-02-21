use crate::blob_storage::BlobStorage;

use super::thread_sync::Sender;
use log::debug;
use super::blob_storage::{Event, EventContent, TaskId, Error};

pub struct AsyncComm {
    pub senders: Vec<Sender<Event>>,
    pub task_id: TaskId
}

pub struct SyncComm<'a> {
    pub events: &'a mut Vec<Event>
}

pub trait Comm {
    fn send_event(&mut self, event: &Event);
    fn task_id(&self) -> TaskId;

    fn send_event_content(&mut self, content: EventContent) {
        let event = Event { id: self.task_id(), content };
        self.send_event(&event);
    }

    fn send_error_event(&mut self, err_msg: String) {
        debug!("Error in task {}: {}", self.task_id().to_u64(), err_msg);
        let event = Event { id: self.task_id(), content: EventContent::Error(Error { msg: err_msg })};
        self.send_event(&event);
    }
}

impl Comm for AsyncComm {
    fn send_event(&mut self, event: &Event) {
        for sender in &self.senders {
            // it's ok if it's disconnected
            let _ = sender.send(event.clone());
        }
    }
    fn task_id(&self) -> TaskId {
        self.task_id
    }
}

impl<'a> Comm for SyncComm<'a> {
    fn send_event(&mut self, event: &Event) {
        self.events.push(event.clone());
    }
    fn task_id(&self) -> TaskId {
        TaskId::from_u64(0)
    }
}

pub fn set_thread_panic_hook() {
    use std::{panic::{set_hook, take_hook}, process::exit};
    let orig_hook = take_hook();
    set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        exit(1);
    }));
}

pub struct TaskHelper {
    senders: Vec<Sender<Event>>,
    next_task_id: u64,
}

pub trait Task : Send {
    fn run<T: Comm>(&mut self, comm: T);
}

impl TaskHelper {
    pub fn new() -> Self {
        Self {
            senders: Vec::new(),
            next_task_id: 0,
        }
    }

    pub fn run_task<T: Task + 'static>(&mut self, mut task: T) -> TaskId {
        let task_id = TaskId::from_u64(self.next_task_id);
        self.next_task_id += 1;

        self.clean_senders();

        let senders = self.senders.clone();

        std::thread::spawn(move || {
            set_thread_panic_hook();
            task.run(AsyncComm { senders, task_id });
        });

        task_id
    }

    pub fn clean_senders(&mut self) {
        let num_senders_before = self.senders.len();
        self.senders.retain(|sender| !sender.disconnected());
        let num_sender_diff = num_senders_before - self.senders.len();
        if num_sender_diff > 0 {
            debug!("Removed {} senders", num_sender_diff);
        }
    }

    pub fn events(&mut self) -> super::thread_sync::Receiver<Event> {
        let (sender, receiver) = super::thread_sync::channel::<Event>();
        self.senders.push(sender);
        receiver
    }
}

pub trait TaskProvider {
    type UploadTask: Task + 'static;
    type DownloadTask: Task + 'static;
    type ExistsTask: Task + 'static;
    fn new_upload_task(&self, data: bytes::Bytes, key: Option<&str>) -> Self::UploadTask;
    fn new_download_task(&self, key: &str) -> Self::DownloadTask;
    fn new_exists_task(&self, key: &str) -> Self::ExistsTask;
    fn task_helper(&mut self) -> &mut TaskHelper;
}

impl<T: TaskProvider> BlobStorage for T {
    fn upload(&mut self, data: bytes::Bytes, key: Option<&str>) -> TaskId {
        let task = self.new_upload_task(data, key);
        self.task_helper().run_task(task)
    }

    fn download(&mut self, key: &str) -> TaskId {
        let task = self.new_download_task(key);
        self.task_helper().run_task(task)
    }

    fn exists(&mut self, key: &str) -> TaskId {
        let task = self.new_exists_task(key);
        self.task_helper().run_task(task)
    }

    fn events(&mut self) -> crate::thread_sync::Receiver<Event> {
        self.task_helper().events()
    }

    fn upload_blocking(&mut self, data: bytes::Bytes, key: Option<&str>) -> crate::blob_storage::UploadResult {

        let mut task = self.new_upload_task(data, key);

        let mut events = Vec::new();
        task.run(SyncComm { events: &mut events });

        for event in &events {
            match &event.content {
                EventContent::UploadSuccess(result) => return Ok(result.clone()),
                EventContent::Error(err) => return Err(err.clone()),
                _ => todo!()
            };
        }

        panic!("Did not find event");
    }

    fn download_blocking(&mut self, key: &str) -> crate::blob_storage::DownloadResult {

        let mut task = self.new_download_task(key);

        let mut events = Vec::new();
        task.run(SyncComm { events: &mut events });

        for event in &events {
            match &event.content {
                EventContent::DownloadSuccess(result) => return Ok(result.clone()),
                EventContent::Error(err) => return Err(err.clone()),
                _ => todo!()
            };
        }

        panic!("Did not find event");
    }

    fn exists_blocking(&mut self, key: &str) -> crate::blob_storage::ExistsResult {

        let mut task = self.new_exists_task(key);

        let mut events = Vec::new();
        task.run(SyncComm { events: &mut events });

        for event in &events {
            match &event.content {
                EventContent::ExistsSuccess(result) => return Ok(*result),
                EventContent::Error(err) => return Err(err.clone()),
                _ => todo!()
            };
        }

        panic!("Did not find event");
    }
}
