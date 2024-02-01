use crate::discovery::{Manifest, PeerDiscovery};
use crate::DynError;
use async_stream::try_stream;
use futures_util::Stream;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::task::JoinHandle;
use zeroconf::bonjour::service::BonjourMdnsService;
use zeroconf::prelude::{TEventLoop, TMdnsBrowser, TMdnsService, TTxtRecord};
use zeroconf::{MdnsBrowser, MdnsService, ServiceDiscovery, ServiceType, TxtRecord};

#[derive(Debug, thiserror::Error)]
pub enum Error {}

pub struct MdnsPeerDiscovery(Arc<Mutex<HashMap<Arc<str>, ServiceHandler>>>);

impl MdnsPeerDiscovery {
    pub fn new() -> Self {
        MdnsPeerDiscovery(Arc::new(Mutex::new(HashMap::new())))
    }
}

#[async_trait::async_trait]
impl PeerDiscovery for MdnsPeerDiscovery {
    fn register(
        &self,
        manifest: &Manifest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Manifest, DynError>> + Send + Sync>>, DynError>
    {
        let registry = Arc::downgrade(&self.0);
        let mut guard = self.0.lock().unwrap();
        let stream = match guard.entry(manifest.peer_id.clone()) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let handle = ServiceHandler::new(manifest, registry)?;
                let stream = handle.subscribe();
                e.insert(handle);
                stream
            }
        };
        Ok(Box::pin(stream))
    }
}

struct ServiceHandler {
    browser_task: JoinHandle<()>,
    service_task: JoinHandle<()>,
    subscription: tokio::sync::broadcast::Sender<Result<Manifest, DynError>>,
}

impl ServiceHandler {
    fn new(
        manifest: &Manifest,
        registry: Weak<Mutex<HashMap<Arc<str>, ServiceHandler>>>,
    ) -> Result<Self, Error> {
        let service_type = ServiceType::new(&manifest.service_name, "udp")?;
        let (subscription, _) = tokio::sync::broadcast::channel(32);
        let mut browser = MdnsBrowser::new(service_type.clone());
        browser.set_service_discovered_callback(Box::new(move |discovered, ctx| {
            let res: Result<Manifest, DynError> = match discovered {
                Ok(s) => parse_manifest(s),
                Err(e) => Err(Box::new(e)),
            };
            let _ = subscription.send(res);
        }));
        let event_loop = browser.browse_services()?;
        let browser_task = tokio::task::spawn_blocking(move || {
            loop {
                // calling `poll()` will keep this service alive
                event_loop.poll(Duration::from_secs(0)).unwrap()
            }
        });

        let mut service = Self::define_service(service_type, manifest);
        let event_loop = service.register()?;
        let service_task = tokio::task::spawn_blocking(move || {
            loop {
                // calling `poll()` will keep this service alive
                event_loop.poll(Duration::from_secs(0)).unwrap()
            }
        });
        Ok(ServiceHandler {
            browser_task,
            service_task,
            subscription,
        })
    }

    fn define_service(service_type: ServiceType, manifest: &Manifest) -> BonjourMdnsService {
        let mut service = MdnsService::new(service_type, manifest.private_endpoint.port());
        service.set_name(&manifest.peer_id);
        service.set_host(&manifest.private_endpoint.ip().to_string());
        let txt_record = TxtRecord::new();
        service.set_txt_record(txt_record);
        service
    }

    fn subscribe(&self) -> impl Stream<Item = Result<Manifest, DynError>> + Send + Sync {
        let mut sub = self.subscription.subscribe();
        try_stream! {
            while let Ok(res) = sub.recv().await {
                yield res;
            }
        }
    }
}

fn parse_manifest(sd: ServiceDiscovery) -> Result<Manifest, DynError> {
    let port = *sd.port();
    let ip = sd.host_name().as_str().parse::<IpAddr>()?;
    let addr = match ip {
        IpAddr::V4(ip) => SocketAddr::V4(SocketAddrV4::new(ip, port)),
        IpAddr::V6(ip) => SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0)),
    };
    Ok(Manifest {
        peer_id: Arc::from(sd.name().as_str()),
        service_name: Arc::from(sd.service_type().name().as_str()),
        private_endpoint: addr,
        public_endpoint: None,
    })
}
