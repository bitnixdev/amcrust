//! Final-write permission composes configuration, source/mux and stream epochs.
use amcrust::media::fmp4::WriteGuard;
use std::sync::{Arc, Mutex};
#[derive(Clone)]
pub struct Lease(Arc<Inner>);
struct Inner {
    current: Mutex<bool>,
    closed: tokio::sync::watch::Sender<bool>,
    parent: Option<Lease>,
    source: Mutex<Option<WriteGuard>>,
}
impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease").finish_non_exhaustive()
    }
}
impl Lease {
    pub fn new() -> Self {
        Self::make(None)
    }
    pub fn child(parent: Lease) -> Self {
        Self::make(Some(parent))
    }
    fn make(parent: Option<Lease>) -> Self {
        Self(Arc::new(Inner {
            current: Mutex::new(true),
            closed: tokio::sync::watch::channel(false).0,
            parent,
            source: Mutex::new(None),
        }))
    }
    pub fn bind(&self, source: WriteGuard) -> Result<(), String> {
        let current = self.0.current.lock().unwrap_or_else(|p| p.into_inner());
        let mut guard = self.0.source.lock().unwrap_or_else(|p| p.into_inner());
        if !*current || guard.is_some() {
            return Err("recording grant cannot be rebound".into());
        }
        *guard = Some(source);
        self.0.closed.send_replace(false);
        Ok(())
    }
    pub fn current(&self) -> bool {
        *self.0.current.lock().unwrap_or_else(|p| p.into_inner())
            && self.0.parent.as_ref().is_none_or(Lease::current)
            && self
                .0
                .source
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .is_none_or(WriteGuard::current)
    }
    pub fn revoke(&self) {
        *self.0.current.lock().unwrap_or_else(|p| p.into_inner()) = false;
        self.0.closed.send_replace(true);
    }
    fn enter(&self, syscall: &mut dyn FnMut()) -> Result<(), String> {
        let current = self.0.current.lock().unwrap_or_else(|p| p.into_inner());
        if !*current {
            return Err("recording output retired".into());
        }
        let source = self.0.source.lock().unwrap_or_else(|p| p.into_inner());
        let mut call = || {
            if let Some(parent) = &self.0.parent {
                parent.enter(syscall)
            } else {
                syscall();
                Ok(())
            }
        };
        if let Some(source) = source.as_ref() {
            source.write(call).map_err(|e| e.to_string())?
        } else {
            call()
        }
    }
    /// Only the final nonblocking syscall belongs inside all retirement gates.
    pub fn write<T>(&self, syscall: impl FnOnce() -> T) -> Result<T, String> {
        let mut syscall = Some(syscall);
        let mut output = None;
        self.enter(&mut || output = Some(syscall.take().unwrap()()))?;
        Ok(output.unwrap())
    }
    pub fn retired(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut closed = self.0.closed.subscribe();
            loop {
                if !self.current() {
                    return;
                }
                let source = self
                    .0
                    .source
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone();
                tokio::select! {
                    _ = closed.changed() => {},
                    _ = async { match &self.0.parent { Some(parent) => parent.retired().await, None => std::future::pending().await } } => return,
                    _ = async { match source { Some(source) => source.retired().await, None => std::future::pending().await } } => return,
                }
            }
        })
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
    #[tokio::test]
    async fn parent_retirement_invalidates_escaped_child_permission() {
        let parent = Lease::new();
        let child = Lease::child(Lease::child(parent.clone()));
        assert_eq!(child.write(|| 7).unwrap(), 7);
        parent.revoke();
        assert!(!child.current());
        assert!(
            child
                .write(|| panic!("retired parent permitted output"))
                .is_err()
        );
        tokio::time::timeout(std::time::Duration::from_millis(50), child.retired())
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
