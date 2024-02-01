use crate::DynError;
use async_stream::stream;
use futures_util::Stream;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::Sender;

pub trait PeerDiscovery: Send + Sync {
    fn register(
        &self,
        manifest: &Manifest,
    ) -> Result<Pin<Box<dyn Stream<Item = Manifest> + Send + Sync>>, DynError>;
}

#[derive(Default, Clone)]
pub struct TestPeerDiscovery {
    inner: Arc<Mutex<TestPeerInner>>,
}

impl PeerDiscovery for TestPeerDiscovery {
    fn register(
        &self,
        manifest: &Manifest,
    ) -> Result<Pin<Box<dyn Stream<Item = Manifest> + Send + Sync>>, DynError> {
        let inner = self.inner.clone();
        let service_name = manifest.service_name.clone();
        let mut receiver = {
            let mut h = inner.lock().unwrap();
            let sender = h
                .services
                .entry(service_name)
                .or_insert_with(|| tokio::sync::broadcast::channel(16).0);
            let _ = sender.send(manifest.clone());
            sender.subscribe()
        };
        Ok(Box::pin(stream! {
            while let Ok(manifest) = receiver.recv().await {
                yield manifest
            }
        }))
    }
}

#[derive(Debug, Default)]
struct TestPeerInner {
    services: HashMap<Arc<str>, Sender<Manifest>>,
}

#[derive(Debug, Clone)]
pub struct Manifest {
    /// Globally unique peer identifier.
    pub peer_id: Arc<str>,
    /// Name of the service. Peers should be discoverable within the same service.
    pub service_name: Arc<str>,
    /// IP:port pair used within the same network.
    pub private_endpoint: SocketAddr,
    /// IP:port assigned when used beyond NAT.
    pub public_endpoint: Option<SocketAddr>,
}
