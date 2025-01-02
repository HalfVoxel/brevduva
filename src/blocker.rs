use tokio::sync::Mutex;
use tokio::sync::Notify;

pub struct Blocker {
    notify: Notify,
    blocked: Mutex<bool>,
}

impl Blocker {
    pub fn new() -> Self {
        Self {
            notify: Notify::new(),
            blocked: Mutex::new(true),
        }
    }

    pub fn new_unblocked() -> Self {
        Self {
            notify: Notify::new(),
            blocked: Mutex::new(false),
        }
    }

    pub async fn wait(&self) {
        let mut blocked = self.blocked.lock().await;
        while *blocked {
            let fut = self.notify.notified();
            tokio::pin!(fut);
            fut.as_mut().enable();

            drop(blocked);
            fut.await;

            blocked = self.blocked.lock().await;
        }
    }

    pub async fn unblock(&self) {
        let mut blocked = self.blocked.lock().await;
        *blocked = false;
        self.notify.notify_waiters();
    }

    #[allow(dead_code)]
    pub fn unblock_blocking(&self) {
        let mut blocked = self.blocked.blocking_lock();
        *blocked = false;
        self.notify.notify_waiters();
    }

    #[allow(dead_code)]
    pub async fn block(&self) {
        *self.blocked.lock().await = true;
    }

    #[allow(dead_code)]
    pub fn block_blocking(&self) {
        *self.blocked.blocking_lock() = true;
    }
}
