//! Recording output epoch: revocation fences the final nonblocking write, not
//! merely access to a fragment before its bytes are copied/encrypted.
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Lease(Arc<Inner>);
#[derive(Debug)]
struct Inner {
    current: Mutex<bool>,
    closed: tokio::sync::watch::Sender<bool>,
}
impl Lease {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            current: Mutex::new(true),
            closed: tokio::sync::watch::channel(false).0,
        }))
    }
    pub fn current(&self) -> bool {
        *self.0.current.lock().unwrap_or_else(|p| p.into_inner())
    }
    pub fn revoke(&self) {
        *self.0.current.lock().unwrap_or_else(|p| p.into_inner()) = false;
        self.0.closed.send_replace(true);
    }
    /// Only a nonblocking syscall belongs inside this closure. Never await or
    /// call arbitrary producer code while holding the retirement gate.
    pub fn write<T>(&self, syscall: impl FnOnce() -> T) -> Result<T, String> {
        let current = self.0.current.lock().unwrap_or_else(|p| p.into_inner());
        if !*current {
            return Err("recording output retired".into());
        }
        Ok(syscall())
    }
    pub async fn retired(&self) {
        let mut closed = self.0.closed.subscribe();
        if *closed.borrow() {
            return;
        }
        let _ = closed.changed().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn retirement_is_sticky_and_copied_bytes_have_no_permission() {
        let lease = Lease::new();
        let copy = lease.clone();
        lease.revoke();
        assert!(!copy.current());
        assert!(copy.write(|| panic!("retired write entered")).is_err());
        tokio::time::timeout(std::time::Duration::from_millis(50), copy.retired())
            .await
            .unwrap();
    }
    #[test]
    fn retirement_waits_for_entered_syscall() {
        let lease = Lease::new();
        let writer = lease.clone();
        let (entered, ready) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let task = std::thread::spawn(move || {
            writer
                .write(|| {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                })
                .unwrap()
        });
        ready.recv().unwrap();
        let retiring = lease.clone();
        let (done, completed) = std::sync::mpsc::channel();
        let stop = std::thread::spawn(move || {
            retiring.revoke();
            done.send(()).unwrap();
        });
        assert!(
            completed
                .recv_timeout(std::time::Duration::from_millis(20))
                .is_err()
        );
        release.send(()).unwrap();
        task.join().unwrap();
        stop.join().unwrap();
        assert!(!lease.current());
    }
}
