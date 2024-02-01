use crate::discovery::{Manifest, PeerDiscovery};
use crate::stream_handle::StreamHandle;
use crate::Result;
use quinn::{crypto, Connecting, Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey, RootCertStore};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::ops::DerefMut;
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use yrs::uuid_v4;

#[derive(Clone)]
pub struct QuicPeer(Arc<InnerPeer>);

impl QuicPeer {
    pub async fn start(mut config: Config) -> Result<Self> {
        let name = config.service_name.clone();
        let server_crypto = if let Some(server_crypto) = config.server_crypto.take() {
            server_crypto
        } else {
            Self::default_server_crypto()?
        };
        let server_config = ServerConfig::with_crypto(server_crypto);
        let server = Endpoint::server(server_config, config.private_addr)?;
        let local_addr = server.local_addr()?;
        tracing::info!(
            "peer `{}` for service `{}` listening on {}",
            config.peer_id,
            name,
            local_addr
        );

        let client_crypto = if let Some(client_crypto) = config.client_crypto.take() {
            client_crypto
        } else {
            Self::default_client_crypto()?
        };
        let mut client = Endpoint::client(local_addr.clone())?;
        client.set_default_client_config(quinn::ClientConfig::new(client_crypto));

        let manifest = Manifest {
            peer_id: config.peer_id,
            service_name: config.service_name,
            private_endpoint: local_addr,
            public_endpoint: None,
        };
        let peer = Arc::new(InnerPeer {
            manifest,
            state: RwLock::new(MutState::new()),
        });
        let manifest = peer.manifest();
        let weak_peer = Arc::downgrade(&peer);

        let _listener_loop = Self::listen(weak_peer.clone(), server);
        for discovery in config.peer_discovery {
            let weak_peer = weak_peer.clone();
            let manifest = manifest.clone();
            let client = client.clone();
            let _discovery_loop = tokio::spawn(async move {
                if let Err(e) = Self::announce(weak_peer, discovery, manifest, client).await {
                    tracing::error!("announcement over peer discovery failed: {}", e);
                }
            });
        }
        Ok(QuicPeer(peer))
    }

    pub fn peer_id(&self) -> &Arc<str> {
        self.0.peer_id()
    }

    fn listen(peer: WeakPeer, server: Endpoint) -> JoinHandle<()> {
        tokio::spawn(async move {
            let local_addr = server.local_addr().unwrap();
            while let Some(conn) = server.accept().await {
                let addr = conn.remote_address();
                tracing::trace!("{} got incoming connection from {}", local_addr, addr);
                let fut = Self::accept_connection(peer.clone(), conn);
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        tracing::error!("connection failed: {}", e)
                    }
                });
            }
        })
    }

    async fn announce(
        peer: WeakPeer,
        discovery: Arc<dyn PeerDiscovery>,
        manifest: Manifest,
        client: Endpoint,
    ) -> Result<()> {
        use futures_util::StreamExt;

        let mut stream = discovery.register(&manifest)?;
        while let Some(incoming_manifest) = stream.next().await {
            let peer = peer.clone();
            let client = client.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::connect(peer, incoming_manifest, client).await {
                    tracing::error!("failed to handle incoming connection: {}", e);
                }
            });
        }
        tracing::debug!("service discovery connection finished gracefully");
        Ok(())
    }

    async fn connect(
        local_peer: WeakPeer,
        remote_manifest: Manifest,
        endpoint: Endpoint,
    ) -> Result<()> {
        let addr = remote_manifest
            .public_endpoint
            .unwrap_or(remote_manifest.private_endpoint);
        let conn = endpoint.connect(addr, &remote_manifest.peer_id)?.await?;
        if let Some(local_peer) = local_peer.upgrade() {
            let peer_id = local_peer.peer_id();
            if peer_id == &remote_manifest.peer_id {
                return Err(crate::Error::PeerNotUnique(peer_id.clone()));
            } else {
                let h = local_peer.state.read().await;
                for (name, handle) in h.stream_handles.iter() {
                    let fut = Self::open_stream(
                        conn.clone(),
                        handle.clone(),
                        peer_id.clone(),
                        name.clone(),
                    );
                    tokio::spawn(fut);
                }
            }
        }
        Ok(())
    }

    async fn open_stream(
        conn: Connection,
        handle: Arc<dyn StreamHandle>,
        peer_id: Arc<str>,
        stream_name: Arc<str>,
    ) -> Result<()> {
        let (mut sender, receiver) = conn.open_bi().await?;
        Self::write_str(&mut sender, &peer_id).await?;
        Self::write_str(&mut sender, &stream_name).await?;
        handle.handle(sender, receiver).await?;
        Ok(())
    }

    pub async fn register_handle<S, H>(&self, stream_name: S, handle: H) -> Result<()>
    where
        S: Into<Arc<str>>,
        H: StreamHandle + 'static,
    {
        let stream_name = stream_name.into();
        if stream_name.len() > u8::MAX as usize {
            return Err(crate::Error::NameTooLong);
        }
        let handle = Arc::new(handle);
        let peer_id = self.0.peer_id().clone();
        let mut inner = self.0.state.write().await;
        match inner.stream_handles.entry(stream_name.clone()) {
            Entry::Occupied(_) => return Err(crate::Error::StreamHandleDefinedAlready),
            Entry::Vacant(e) => e.insert(handle.clone()),
        };
        for (remote_peer_id, conn) in inner.active_connections.iter() {
            let name = stream_name.clone();
            let peer_id = peer_id.clone();
            let remote_peer_id = remote_peer_id.clone();
            let conn = conn.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                tracing::trace!(
                    "creating stream `{}` between `{}`->`{}`",
                    name,
                    peer_id,
                    remote_peer_id
                );
                if let Err(e) = Self::handle_connection(handle, &conn, &peer_id, &name).await {
                    tracing::trace!(
                        "stream `{}` between `{}`->`{}` failed: {}",
                        name,
                        peer_id,
                        remote_peer_id,
                        e
                    );
                }
            });
        }
        Ok(())
    }

    async fn handle_connection(
        handle: Arc<dyn StreamHandle>,
        conn: &Connection,
        peer_id: &Arc<str>,
        stream_name: &Arc<str>,
    ) -> Result<()> {
        let (mut sender, receiver) = conn.open_bi().await?;
        Self::write_str(&mut sender, peer_id).await?;
        Self::write_str(&mut sender, stream_name).await?;
        handle.handle(sender, receiver).await?;
        Ok(())
    }

    fn default_server_crypto() -> Result<Arc<dyn crypto::ServerConfig>> {
        let builder = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth();
        let cert = generate_simple_self_signed(vec!["localhost".to_string()])?;
        let private_key = PrivateKey(cert.serialize_private_key_der());
        let certificate = Certificate(cert.serialize_der()?);
        Ok(Arc::new(
            builder.with_single_cert(vec![certificate], private_key)?,
        ))
    }

    fn default_client_crypto() -> Result<Arc<dyn crypto::ClientConfig>> {
        let builder = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        Ok(Arc::new(builder))
    }

    async fn accept_connection(peer: Weak<InnerPeer>, connecting: Connecting) -> Result<()> {
        let conn = connecting.await?;
        tracing::trace!("connection established with {}", conn.remote_address());
        loop {
            let result = conn.accept_bi().await;
            let (sender, mut receiver) = match result {
                Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                    tracing::info!("connection closed: {}", conn.remote_address());
                    return Ok(());
                }
                Err(e) => {
                    return Err(e.into());
                }
                Ok(s) => s,
            };
            let peer_id = Self::read_str(&mut receiver).await?;
            let name = Self::read_str(&mut receiver).await?;
            let stream_handle = if let Some(peer) = peer.upgrade() {
                let mut h = peer.state.write().await;
                let h = h.deref_mut();
                if let Some(_old) = h.active_connections.insert(peer_id, conn.clone()) {
                    todo!("reset older stream handles")
                }
                match h.stream_handles.entry(name.clone()) {
                    Entry::Occupied(e) => e.get().clone(),
                    Entry::Vacant(_) => {
                        tracing::error!("received stream named `{}` but no handle has been defined for it. Dropping", name);
                        continue;
                    }
                }
            } else {
                return Ok(()); // current peer has already been dropped
            };
            let peer = peer.clone();
            tokio::spawn(async move {
                if let Err(e) = stream_handle.handle(sender, receiver).await {
                    tracing::error!("stream failed: {}", e);
                    if let Some(peer) = peer.upgrade() {
                        let mut h = peer.state.write().await;
                        h.stream_handles.remove(&name);
                    }
                }
            });
        }
    }

    async fn read_str(receiver: &mut RecvStream) -> Result<Arc<str>> {
        let mut len = [0u8; 1];
        receiver.read_exact(&mut len).await?;
        let len = len[0] as usize;
        let mut buf = vec![0u8; len];
        receiver.read_exact(&mut buf).await?;
        let stream_name = String::from_utf8(buf).map_err(|e| crate::Error::Other(Box::new(e)))?;
        Ok(Arc::from(stream_name))
    }

    async fn write_str(sender: &mut SendStream, str: &str) -> Result<()> {
        let len = str.len();
        if len > u8::MAX as usize {
            return Err(crate::Error::NameTooLong);
        }
        sender.write_all(&[len as u8]).await?;
        sender.write_all(str.as_bytes()).await?;
        Ok(())
    }
}

type WeakPeer = Weak<InnerPeer>;

struct InnerPeer {
    manifest: Manifest,
    state: RwLock<MutState>,
}

impl InnerPeer {
    fn peer_id(&self) -> &Arc<str> {
        &self.manifest.peer_id
    }

    fn local_addr(&self) -> &SocketAddr {
        &self.manifest.private_endpoint
    }

    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}

struct MutState {
    active_connections: HashMap<Arc<str>, Connection>,
    stream_handles: HashMap<Arc<str>, Arc<dyn StreamHandle>>,
}

impl MutState {
    fn new() -> Self {
        MutState {
            active_connections: HashMap::new(),
            stream_handles: HashMap::new(),
        }
    }
}

pub struct Config {
    /// List of peer discovery services used to announce and discover each other.
    pub peer_discovery: Vec<Arc<dyn PeerDiscovery>>,
    pub server_crypto: Option<Arc<dyn crypto::ServerConfig>>,
    pub client_crypto: Option<Arc<dyn crypto::ClientConfig>>,
    pub private_addr: SocketAddr,
    pub service_name: Arc<str>,
    pub peer_id: Arc<str>,
}

impl Config {
    pub fn new<S>(service_name: S, port: u16) -> Self
    where
        S: Into<Arc<str>>,
    {
        Config {
            peer_discovery: Vec::default(),
            peer_id: Self::random_peer_id(),
            server_crypto: None,
            client_crypto: None,
            private_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)),
            service_name: service_name.into(),
        }
    }

    pub fn random_peer_id() -> Arc<str> {
        let peer_id = uuid_v4(&mut rand::thread_rng());
        Arc::from(peer_id.to_string())
    }

    pub fn add_peer_discovery<D>(&mut self, discovery: D)
    where
        D: PeerDiscovery + 'static,
    {
        self.peer_discovery.push(Arc::new(discovery));
    }
}

#[cfg(test)]
mod test {
    use crate::discovery::TestPeerDiscovery;
    use crate::peer::{Config, QuicPeer};
    use crate::stream_handle::AwarenessHandle;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::time::sleep;
    use y_sync::awareness::Awareness;
    use y_sync::net::BroadcastGroup;
    use yrs::{Doc, GetString, Text, Transact};

    async fn create_handle(awareness: Arc<RwLock<Awareness>>) -> (Arc<str>, AwarenessHandle) {
        let doc = Doc::new();
        let id: Arc<str> = Arc::from(doc.guid().to_string());
        let group = BroadcastGroup::new(awareness, 64).await;
        let handle = AwarenessHandle::new(id.clone(), group);
        (id, handle)
    }

    fn config(d: TestPeerDiscovery) -> Config {
        let mut config = Config::new("test", 0);
        config.add_peer_discovery(d);
        config
    }

    #[tokio::test]
    async fn test_basic_yrs() {
        let discovery = TestPeerDiscovery::default();
        let p1 = QuicPeer::start(config(discovery.clone())).await.unwrap();
        let p2 = QuicPeer::start(config(discovery.clone())).await.unwrap();
        let p3 = QuicPeer::start(config(discovery)).await.unwrap();

        let a1: Arc<RwLock<Awareness>> = Arc::default();
        let (id, handle) = create_handle(a1.clone()).await;
        p1.register_handle(id, handle).await.unwrap();

        let a2: Arc<RwLock<Awareness>> = Arc::default();
        let (id, handle) = create_handle(a2.clone()).await;
        p2.register_handle(id, handle).await.unwrap();

        let a3: Arc<RwLock<Awareness>> = Arc::default();
        let (id, handle) = create_handle(a3.clone()).await;
        p3.register_handle(id, handle).await.unwrap();

        {
            let awareness = a1.write().await;
            let doc = awareness.doc();
            let txt = doc.get_or_insert_text("text");
            txt.push(&mut doc.transact_mut(), "hello world");
        }

        sleep(Duration::from_millis(100)).await;

        {
            let awareness = a2.write().await;
            let doc = awareness.doc();
            let txt = doc.get_or_insert_text("text");
            assert_eq!(&txt.get_string(&doc.transact()), "hello world");
        }

        {
            let awareness = a3.write().await;
            let doc = awareness.doc();
            let txt = doc.get_or_insert_text("text");
            assert_eq!(&txt.get_string(&doc.transact()), "hello world");
        }
    }

    #[tokio::test]
    async fn test_basic_zeroconf() {
        // test yrs using zeroconf service discovery
        todo!()
    }

    #[tokio::test]
    async fn test_basic_non_yrs() {
        // test quicpeer using stream handle other than yrs
        todo!()
    }

    #[tokio::test]
    async fn test_dynamic_stream_handling() {
        // test yrs dynamic doc creation for streams that have not been registered locally
        todo!()
    }
}
