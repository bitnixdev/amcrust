//! Stop requests while serving, including the normal Unix service-manager signal.
use std::io;

#[cfg(unix)]
pub struct StopSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}
#[cfg(unix)]
impl StopSignals {
    pub fn install() -> io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }
    pub async fn wait(&mut self) -> io::Result<&'static str> {
        tokio::select! {
            s = self.interrupt.recv() => s.map(|_| "SIGINT"),
            s = self.terminate.recv() => s.map(|_| "SIGTERM"),
        }
        .ok_or_else(|| io::Error::other("shutdown signal stream closed"))
    }
}
#[cfg(not(unix))]
pub struct StopSignals;
#[cfg(not(unix))]
impl StopSignals {
    pub fn install() -> io::Result<Self> {
        Ok(Self)
    }
    pub async fn wait(&mut self) -> io::Result<&'static str> {
        tokio::signal::ctrl_c().await?;
        Ok("interrupt")
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Command;
    // Exercise actual OS delivery only in a dedicated child test process.
    #[tokio::test]
    async fn serving_stop_signals() {
        const KEY: &str = "AMCRUST_TEST_STOP_SIGNAL";
        if let Ok(signal) = std::env::var(KEY) {
            assert!(matches!(signal.as_str(), "INT" | "TERM"));
            let mut signals = StopSignals::install().unwrap();
            assert!(
                Command::new("sh")
                    .args([
                        "-c",
                        "kill -s \"$1\" \"$2\"",
                        "signal-test",
                        &signal,
                        &std::process::id().to_string()
                    ])
                    .status()
                    .unwrap()
                    .success()
            );
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(3), signals.wait())
                    .await
                    .unwrap()
                    .unwrap(),
                format!("SIG{signal}")
            );
        } else {
            for signal in ["INT", "TERM"] {
                assert!(
                    Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", "service_stop::tests::serving_stop_signals"])
                        .env(KEY, signal)
                        .status()
                        .unwrap()
                        .success()
                );
            }
        }
    }
}
