use libmdns::{Responder, Service};
use log::debug;

use crate::pointer;

/// An mDNS Responder. Used to announce the Accessory's name and HAP TXT records to potential controllers.
pub struct MdnsResponder {
    config: pointer::Config,
    responder: Responder,
    service: Option<Service>,
    task: Option<Box<dyn futures::Future<Output = ()> + Unpin + std::marker::Send>>,
    hostname: String,
    allowed_ips: Vec<std::net::IpAddr>,
}

impl MdnsResponder {
    /// Creates a new mDNS Responder.
    ///
    /// The responder advertises a hostname unique to this accessory (derived
    /// from the accessory name) instead of the machine hostname, so it never
    /// conflicts with the host system's own mDNS responder, and only reports
    /// the server's configured IP.
    pub async fn new(config: pointer::Config) -> Self {
        let c = config.lock().await;
        let hostname = accessory_hostname(&c.name);
        let allowed_ips = vec![c.host];
        drop(c);

        let (responder, task) =
            Responder::with_default_handle_and_ip_list_and_hostname(allowed_ips.clone(), hostname.clone())
                .expect("creating mDNS responder");

        MdnsResponder {
            config,
            responder,
            service: None,
            task: Some(task),
            hostname,
            allowed_ips,
        }
    }

    /// Derives new mDNS TXT records from the server's `Config`.
    pub async fn update_records(&mut self) {
        debug!("attempting to set mDNS records");

        self.service = None;

        let c = self.config.lock().await;

        let name = c.name.clone();
        let port = c.port;
        let tr = c.txt_records();

        drop(c);

        self.service = Some(self.responder.register("_hap._tcp".into(), name, port, &[
            &tr[0], &tr[1], &tr[2], &tr[3], &tr[4], &tr[5], &tr[6], &tr[7],
        ]));

        debug!("setting mDNS records: {:?}", &tr);
    }

    /// Returns the mDNS task to throw on a scheduler.
    pub fn run_handle(&mut self) -> Box<dyn futures::Future<Output = ()> + Unpin + std::marker::Send> {
        match self.task.take() {
            Some(task) => task,
            // if the task handle is gone, recreate the whole responder
            None => {
                let (responder, task) = Responder::with_default_handle_and_ip_list_and_hostname(
                    self.allowed_ips.clone(),
                    self.hostname.clone(),
                )
                .expect("creating mDNS responder");
                self.responder = responder;

                task
            },
        }
    }
}

/// A unique mDNS hostname for this accessory, e.g. "hap-frontyard.local".
fn accessory_hostname(name: &str) -> String {
    let slug: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    format!("hap-{}", slug.trim_matches('-'))
}
