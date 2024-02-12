use std::sync::{atomic::{AtomicBool, Ordering}, mpsc, Arc};

pub struct Receiver<T> {
    pub inner: mpsc::Receiver<T>,
    disconnect: Arc<AtomicBool>
}

#[derive(Clone)]
pub struct Sender<T> {
    pub inner: mpsc::Sender<T>,
    disconnect: Arc<AtomicBool>
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (mpsc_send, mpsc_rec) = mpsc::channel();
    let disconnect = Arc::new(AtomicBool::new(false));
    let sender = Sender {
        inner: mpsc_send,
        disconnect: disconnect.clone()
    };
    let receiver = Receiver {
        inner: mpsc_rec,
        disconnect
    };
    (sender, receiver)
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.disconnect.store(true, Ordering::Release);
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, mpsc::TryRecvError> {
        self.inner.try_recv()
    }

    pub fn recv(&self) -> Result<T, mpsc::RecvError> {
        self.inner.recv()
    }
}

impl<T> Sender<T> {
    pub fn send(&self, t: T) -> Result<(), mpsc::SendError<T>> {
        self.inner.send(t)
    }

    pub fn disconnected(&self) -> bool {
        self.disconnect.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnect() {
        let (sender, receiver) = channel::<()>();
        assert!(!sender.disconnected());
        drop(receiver);
        assert!(sender.disconnected());
    }
}