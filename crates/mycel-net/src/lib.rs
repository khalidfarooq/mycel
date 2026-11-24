use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tracing::{debug, info, warn};

use mycel_crypto::{
    derive_session_ciphers, CryptoError, EphemeralKeyPair, HandshakeRole, NodeIdentity, OpenCipher,
    SealCipher,
};
use mycel_proto::{
    HardwareProfile, MeshMessage, PeerDiscoveryBeacon, PeerHeartbeat, SignedHandshake,
};

const MAX_HANDSHAKE_BYTES: usize = 1024 * 1024;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 77, 77);

#[derive(Error, Debug)]
pub enum NetError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Crypto error: {0}")]
    Crypto(#[from] CryptoError),
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),
}

#[derive(Debug, Clone)]
pub enum MeshEvent {
    PeerDiscovered {
        peer: PeerHeartbeat,
        addr: SocketAddr,
    },
    PeerLost {
        peer_id: String,
    },
    MessageReceived {
        from_peer_id: String,
        message: MeshMessage,
    },
}

#[derive(Clone)]
pub struct NetworkConfig {
    pub bind_ip: String,
    pub tcp_port: u16,
    pub discovery_port: u16,
    pub heartbeat_interval: Duration,
    pub enable_udp_broadcast: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_ip: "127.0.0.1".into(),
            tcp_port: 0,
            discovery_port: 8766,
            heartbeat_interval: Duration::from_millis(500),
            enable_udp_broadcast: false,
        }
    }
}

#[derive(Clone)]
struct ConnectionShared {
    identity: NodeIdentity,
    node_name: String,
    hardware: HardwareProfile,
    actual_tcp_addr: SocketAddr,
    event_tx: mpsc::Sender<MeshEvent>,
    active_connections: Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>>,
    connecting_peers: Arc<Mutex<HashSet<String>>>,
    peers: Arc<RwLock<HashMap<String, SocketAddr>>>,
}

pub struct MeshNetwork {
    pub identity: NodeIdentity,
    pub node_name: String,
    pub hardware: HardwareProfile,
    pub actual_tcp_addr: SocketAddr,
    pub config: NetworkConfig,
    event_rx: Arc<Mutex<mpsc::Receiver<MeshEvent>>>,
    shared: ConnectionShared,
    shutdown_tx: watch::Sender<bool>,
}

impl Drop for MeshNetwork {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl MeshNetwork {
    pub async fn start(
        identity: NodeIdentity,
        node_name: impl Into<String>,
        hardware: HardwareProfile,
        config: NetworkConfig,
    ) -> Result<Self, NetError> {
        let node_name = node_name.into();
        let tcp_listener =
            TcpListener::bind(format!("{}:{}", config.bind_ip, config.tcp_port)).await?;
        let actual_tcp_addr = tcp_listener.local_addr()?;
        info!(addr = %actual_tcp_addr, node = %node_name, "Mycel TCP listener bound");

        let (event_tx, event_rx) = mpsc::channel::<MeshEvent>(1000);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let shared = ConnectionShared {
            identity: identity.clone(),
            node_name: node_name.clone(),
            hardware: hardware.clone(),
            actual_tcp_addr,
            event_tx,
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            connecting_peers: Arc::new(Mutex::new(HashSet::new())),
            peers: Arc::new(RwLock::new(HashMap::new())),
        };

        // Inbound TCP acceptor loop
        {
            let shared_accept = shared.clone();
            let mut shut_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shut_rx.changed() => {
                            if *shut_rx.borrow() {
                                break;
                            }
                        }
                        res = tcp_listener.accept() => {
                            match res {
                                Ok((stream, remote_addr)) => {
                                    let s = shared_accept.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = Self::handle_inbound_stream(stream, remote_addr, s).await {
                                            debug!(error = %e, remote = %remote_addr, "Inbound peer handshake rejected");
                                        }
                                    });
                                }
                                Err(e) => {
                                    warn!(error = %e, "TCP accept error");
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }

        // UDP LAN Peer Auto-Discovery (Broadcaster + Receiver)
        if config.enable_udp_broadcast {
            Self::spawn_udp_discovery(&config, &shared, &shutdown_rx)?;
        }

        Ok(Self {
            identity,
            node_name,
            hardware,
            actual_tcp_addr,
            config,
            event_rx: Arc::new(Mutex::new(event_rx)),
            shared,
            shutdown_tx,
        })
    }

    fn spawn_udp_discovery(
        config: &NetworkConfig,
        shared: &ConnectionShared,
        shutdown_rx: &watch::Receiver<bool>,
    ) -> Result<(), NetError> {
        let rx_sock = create_reuse_udp_socket(config.discovery_port)?;
        let tx_sock = create_sender_udp_socket(&config.bind_ip)?;

        let discovery_port = config.discovery_port;
        let bind_ip = config.bind_ip.clone();
        let interval = config.heartbeat_interval;

        // Receiver loop
        {
            let shared_rx = shared.clone();
            let mut shut_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65535];
                loop {
                    tokio::select! {
                        _ = shut_rx.changed() => {
                            if *shut_rx.borrow() {
                                break;
                            }
                        }
                        res = rx_sock.recv_from(&mut buf) => {
                            match res {
                                Ok((len, src_addr)) => {
                                    if let Ok(beacon) = bincode::deserialize::<PeerDiscoveryBeacon>(&buf[..len]) {
                                        Self::handle_discovery_beacon(beacon, src_addr, &shared_rx).await;
                                    }
                                }
                                Err(e) => {
                                    debug!(error = %e, "UDP discovery recv error");
                                }
                            }
                        }
                    }
                }
            });
        }

        // Broadcaster loop
        {
            let shared_tx = shared.clone();
            let mut shut_tx = shutdown_rx.clone();
            tokio::spawn(async move {
                let mcast_target = SocketAddr::V4(SocketAddrV4::new(MULTICAST_ADDR, discovery_port));
                let bcast_target =
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, discovery_port));
                let send_lan_bcast = bind_ip != "127.0.0.1";

                loop {
                    let mut beacon = PeerDiscoveryBeacon {
                        peer_id: shared_tx.identity.peer_id().to_string(),
                        public_key_hex: shared_tx.identity.public_key_hex(),
                        node_name: shared_tx.node_name.clone(),
                        actual_tcp_addr: shared_tx.actual_tcp_addr,
                        hardware: shared_tx.hardware.clone(),
                        timestamp: chrono::Utc::now().timestamp_millis(),
                        signature: String::new(),
                    };
                    beacon.signature = shared_tx.identity.sign(&beacon.signing_payload());

                    if let Ok(bytes) = bincode::serialize(&beacon) {
                        let _ = tx_sock.send_to(&bytes, mcast_target).await;
                        if send_lan_bcast {
                            let _ = tx_sock.send_to(&bytes, bcast_target).await;
                        }
                    }

                    tokio::select! {
                        _ = shut_tx.changed() => {
                            if *shut_tx.borrow() {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(interval) => {}
                    }
                }
            });
        }

        Ok(())
    }

    async fn handle_discovery_beacon(
        beacon: PeerDiscoveryBeacon,
        src_addr: SocketAddr,
        shared: &ConnectionShared,
    ) {
        if beacon.peer_id == shared.identity.peer_id() {
            return;
        }

        if NodeIdentity::verify_peer_id(&beacon.peer_id, &beacon.public_key_hex).is_err() {
            return;
        }
        match NodeIdentity::verify_signature(
            &beacon.public_key_hex,
            &beacon.signing_payload(),
            &beacon.signature,
        ) {
            Ok(true) => {}
            _ => return,
        }

        // Deterministic peer_id tie-breaker prevents simultaneous bidirectional connect races
        if shared.identity.peer_id() >= beacon.peer_id.as_str() {
            return;
        }

        if shared
            .active_connections
            .lock()
            .await
            .contains_key(&beacon.peer_id)
        {
            return;
        }

        {
            let mut connecting = shared.connecting_peers.lock().await;
            if !connecting.insert(beacon.peer_id.clone()) {
                return;
            }
        }

        let connect_addr = if beacon.actual_tcp_addr.ip().is_unspecified() {
            SocketAddr::new(src_addr.ip(), beacon.actual_tcp_addr.port())
        } else {
            beacon.actual_tcp_addr
        };

        let s = shared.clone();
        let peer_id = beacon.peer_id.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::connect_peer_shared(connect_addr, &s).await {
                debug!(peer_id = %peer_id, addr = %connect_addr, error = %e, "Auto-discovery connect attempt failed");
            }
            s.connecting_peers.lock().await.remove(&peer_id);
        });
    }

    /// Connect directly to a peer at a known TCP address using X25519 + Ed25519 + ChaCha20-Poly1305.
    pub async fn connect_peer(&self, addr: SocketAddr) -> Result<(), NetError> {
        Self::connect_peer_shared(addr, &self.shared).await
    }

    async fn connect_peer_shared(
        addr: SocketAddr,
        shared: &ConnectionShared,
    ) -> Result<(), NetError> {
        let stream = TcpStream::connect(addr).await?;
        let (mut reader, mut writer) = stream.into_split();

        let my_ephem = EphemeralKeyPair::generate()?;
        let my_ephem_pub = my_ephem.public_key_bytes;
        let my_hs = build_signed_handshake(shared, my_ephem_pub);
        let init_bytes = bincode::serialize(&my_hs)?;
        write_plain_frame(&mut writer, &init_bytes).await?;

        let remote_bytes = read_plain_frame(&mut reader, MAX_HANDSHAKE_BYTES).await?;
        let remote_hs: SignedHandshake = bincode::deserialize(&remote_bytes)?;
        verify_remote_handshake(&remote_hs, shared.identity.peer_id())?;

        let (seal_cipher, open_cipher) = derive_session_ciphers(
            my_ephem,
            &remote_hs.ephemeral_pubkey,
            shared.identity.peer_id(),
            &remote_hs.peer_id,
            &my_ephem_pub,
            &remote_hs.ephemeral_pubkey,
            HandshakeRole::Initiator,
        )?;

        let remote_peer_id = remote_hs.peer_id.clone();
        let reported_addr = if remote_hs.tcp_listen_port > 0 {
            SocketAddr::new(addr.ip(), remote_hs.tcp_listen_port)
        } else {
            addr
        };

        spawn_encrypted_session(
            reader,
            writer,
            seal_cipher,
            open_cipher,
            remote_hs.heartbeat,
            reported_addr,
            shared.clone(),
        )
        .await;

        debug!(peer_id = %remote_peer_id, addr = %addr, "Encrypted outbound session established");
        Ok(())
    }

    async fn handle_inbound_stream(
        stream: TcpStream,
        remote_addr: SocketAddr,
        shared: ConnectionShared,
    ) -> Result<(), NetError> {
        let (mut reader, mut writer) = stream.into_split();

        let remote_bytes = read_plain_frame(&mut reader, MAX_HANDSHAKE_BYTES).await?;
        let remote_hs: SignedHandshake = bincode::deserialize(&remote_bytes)?;
        verify_remote_handshake(&remote_hs, shared.identity.peer_id())?;

        let my_ephem = EphemeralKeyPair::generate()?;
        let my_ephem_pub = my_ephem.public_key_bytes;
        let my_hs = build_signed_handshake(&shared, my_ephem_pub);
        let resp_bytes = bincode::serialize(&my_hs)?;
        write_plain_frame(&mut writer, &resp_bytes).await?;

        let (seal_cipher, open_cipher) = derive_session_ciphers(
            my_ephem,
            &remote_hs.ephemeral_pubkey,
            &remote_hs.peer_id,
            shared.identity.peer_id(),
            &remote_hs.ephemeral_pubkey,
            &my_ephem_pub,
            HandshakeRole::Responder,
        )?;

        let reported_addr = if remote_hs.tcp_listen_port > 0 {
            SocketAddr::new(remote_addr.ip(), remote_hs.tcp_listen_port)
        } else {
            remote_addr
        };

        spawn_encrypted_session(
            reader,
            writer,
            seal_cipher,
            open_cipher,
            remote_hs.heartbeat,
            reported_addr,
            shared,
        )
        .await;

        Ok(())
    }

    /// Broadcast message to all connected peers in mesh
    pub async fn broadcast(&self, message: MeshMessage) -> Result<(), NetError> {
        let bytes = message.to_bytes()?;
        let senders: Vec<mpsc::Sender<Vec<u8>>> = {
            let map = self.shared.active_connections.lock().await;
            map.values().cloned().collect()
        };
        for tx in senders {
            let _ = tx.send(bytes.clone()).await;
        }
        Ok(())
    }

    /// Send message directly to a target peer
    pub async fn send_direct(
        &self,
        target_peer_id: &str,
        message: MeshMessage,
    ) -> Result<(), NetError> {
        let bytes = message.to_bytes()?;
        let tx_opt = {
            let map = self.shared.active_connections.lock().await;
            map.get(target_peer_id).cloned()
        };
        if let Some(tx) = tx_opt {
            let _ = tx.send(bytes).await;
        }
        Ok(())
    }

    /// Number of currently connected encrypted peers
    pub async fn connected_peer_count(&self) -> usize {
        self.shared.active_connections.lock().await.len()
    }

    /// Fetch next network event
    pub async fn next_event(&self) -> Option<MeshEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await
    }
}

fn build_signed_handshake(shared: &ConnectionShared, ephemeral_pubkey: [u8; 32]) -> SignedHandshake {
    let peer_id = shared.identity.peer_id().to_string();
    let public_key_hex = shared.identity.public_key_hex();
    let tcp_listen_port = shared.actual_tcp_addr.port();

    let payload = SignedHandshake::signing_payload(
        &peer_id,
        &public_key_hex,
        &ephemeral_pubkey,
        tcp_listen_port,
    );
    let signature = shared.identity.sign(&payload);

    SignedHandshake {
        peer_id: peer_id.clone(),
        public_key_hex,
        ephemeral_pubkey,
        tcp_listen_port,
        heartbeat: PeerHeartbeat {
            peer_id,
            node_name: shared.node_name.clone(),
            hardware: shared.hardware.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            uptime_secs: 0,
            active_tasks: 0,
        },
        signature,
    }
}

fn verify_remote_handshake(hs: &SignedHandshake, self_peer_id: &str) -> Result<(), NetError> {
    if hs.peer_id == self_peer_id {
        return Err(NetError::HandshakeFailed("Self-connection rejected".into()));
    }
    if hs.heartbeat.peer_id != hs.peer_id {
        return Err(NetError::HandshakeFailed(
            "Heartbeat peer_id mismatch".into(),
        ));
    }
    NodeIdentity::verify_peer_id(&hs.peer_id, &hs.public_key_hex)
        .map_err(|e| NetError::HandshakeFailed(e.to_string()))?;

    let payload = SignedHandshake::signing_payload(
        &hs.peer_id,
        &hs.public_key_hex,
        &hs.ephemeral_pubkey,
        hs.tcp_listen_port,
    );
    let valid = NodeIdentity::verify_signature(&hs.public_key_hex, &payload, &hs.signature)
        .map_err(|e| NetError::HandshakeFailed(e.to_string()))?;
    if !valid {
        return Err(NetError::HandshakeFailed(
            "Invalid Ed25519 handshake signature".into(),
        ));
    }
    Ok(())
}

async fn write_plain_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    bytes: &[u8],
) -> Result<(), NetError> {
    let len = (bytes.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(bytes).await?;
    Ok(())
}

async fn read_plain_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> Result<Vec<u8>, NetError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;
    if msg_len == 0 || msg_len > max_len {
        return Err(NetError::HandshakeFailed(format!(
            "Invalid frame length: {}",
            msg_len
        )));
    }
    let mut buf = vec![0u8; msg_len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn spawn_encrypted_session(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut seal_cipher: SealCipher,
    mut open_cipher: OpenCipher,
    remote_hb: PeerHeartbeat,
    reported_addr: SocketAddr,
    shared: ConnectionShared,
) {
    let remote_peer_id = remote_hb.peer_id.clone();
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(256);

    shared
        .active_connections
        .lock()
        .await
        .insert(remote_peer_id.clone(), tx);
    shared
        .peers
        .write()
        .await
        .insert(remote_peer_id.clone(), reported_addr);

    let _ = shared
        .event_tx
        .send(MeshEvent::PeerDiscovered {
            peer: remote_hb,
            addr: reported_addr,
        })
        .await;

    // Encrypted writer loop
    tokio::spawn(async move {
        while let Some(plaintext) = rx.recv().await {
            let Ok(ciphertext) = seal_cipher.seal(&plaintext) else {
                break;
            };
            let len = (ciphertext.len() as u32).to_be_bytes();
            if writer.write_all(&len).await.is_err() || writer.write_all(&ciphertext).await.is_err()
            {
                break;
            }
        }
    });

    // Encrypted reader loop
    let ev_tx = shared.event_tx.clone();
    let conn_map = shared.active_connections.clone();
    let peers_map = shared.peers.clone();
    let pid_clone = remote_peer_id.clone();

    tokio::spawn(async move {
        while let Ok(ciphertext) = read_plain_frame(&mut reader, MAX_FRAME_BYTES).await {
            match open_cipher.open(&ciphertext) {
                Ok(plaintext) => {
                    if let Ok(msg) = MeshMessage::from_bytes(&plaintext) {
                        let _ = ev_tx
                            .send(MeshEvent::MessageReceived {
                                from_peer_id: pid_clone.clone(),
                                message: msg,
                            })
                            .await;
                    }
                }
                Err(e) => {
                    warn!(peer_id = %pid_clone, error = %e, "AEAD decryption failed; closing connection");
                    break;
                }
            }
        }
        conn_map.lock().await.remove(&pid_clone);
        peers_map.write().await.remove(&pid_clone);
        let _ = ev_tx
            .send(MeshEvent::PeerLost {
                peer_id: pid_clone,
            })
            .await;
    });
}

fn create_reuse_udp_socket(port: u16) -> Result<UdpSocket, NetError> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    sock.set_reuse_port(true)?;

    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    sock.bind(&bind_addr.into())?;

    // Join multicast on loopback (reliable on CI/local) and default interface (LAN)
    let _ = sock.join_multicast_v4(&MULTICAST_ADDR, &Ipv4Addr::LOCALHOST);
    let _ = sock.join_multicast_v4(&MULTICAST_ADDR, &Ipv4Addr::UNSPECIFIED);

    sock.set_nonblocking(true)?;
    let std_sock: std::net::UdpSocket = sock.into();
    Ok(UdpSocket::from_std(std_sock)?)
}

fn create_sender_udp_socket(bind_ip: &str) -> Result<UdpSocket, NetError> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_broadcast(true)?;
    sock.set_multicast_loop_v4(true)?;

    let ip: Ipv4Addr = bind_ip.parse().unwrap_or(Ipv4Addr::LOCALHOST);
    let _ = sock.set_multicast_if_v4(&ip);
    sock.bind(&SocketAddrV4::new(ip, 0).into())?;
    sock.set_nonblocking(true)?;
    let std_sock: std::net::UdpSocket = sock.into();
    Ok(UdpSocket::from_std(std_sock)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycel_proto::{CrdtDelta, TaskKind, TaskSpec};

    #[tokio::test]
    async fn test_mesh_network_mutual_handshake() {
        let id1 = NodeIdentity::generate();
        let id2 = NodeIdentity::generate();

        let hw = HardwareProfile::default();
        let cfg1 = NetworkConfig::default();
        let cfg2 = NetworkConfig::default();

        let net1 = MeshNetwork::start(id1.clone(), "node_alpha", hw.clone(), cfg1)
            .await
            .unwrap();
        let net2 = MeshNetwork::start(id2.clone(), "node_beta", hw.clone(), cfg2)
            .await
            .unwrap();

        net2.connect_peer(net1.actual_tcp_addr)
            .await
            .expect("Connect peer should succeed");

        let ev1 = net1.next_event().await.unwrap();
        match ev1 {
            MeshEvent::PeerDiscovered { peer, .. } => {
                assert_eq!(peer.peer_id, id2.peer_id());
            }
            _ => panic!("Expected Net1 to discover Net2"),
        }

        let ev2 = net2.next_event().await.unwrap();
        match ev2 {
            MeshEvent::PeerDiscovered { peer, .. } => {
                assert_eq!(peer.peer_id, id1.peer_id());
            }
            _ => panic!("Expected Net2 to discover Net1"),
        }

        net1.send_direct(
            id2.peer_id(),
            MeshMessage::Delta(CrdtDelta {
                key: "encrypted_check".into(),
                value: b"secret_value".to_vec(),
                origin_peer_id: id1.peer_id().to_string(),
                lamport_clock: 1,
                timestamp: 100,
            }),
        )
        .await
        .unwrap();

        let ev_msg = net2.next_event().await.unwrap();
        match ev_msg {
            MeshEvent::MessageReceived {
                from_peer_id,
                message: MeshMessage::Delta(delta),
            } => {
                assert_eq!(from_peer_id, id1.peer_id());
                assert_eq!(delta.value, b"secret_value");
            }
            _ => panic!("Expected encrypted Delta MessageReceived on Net2"),
        }
    }

    #[tokio::test]
    async fn test_wire_bytes_are_encrypted_and_tampering_rejected() {
        // Relay TCP proxy captures raw wire bytes and tests tampering
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();

        let server_id = NodeIdentity::generate();
        let client_id = NodeIdentity::generate();
        let hw = HardwareProfile::default();

        let server = MeshNetwork::start(
            server_id.clone(),
            "server",
            hw.clone(),
            NetworkConfig::default(),
        )
        .await
        .unwrap();
        let server_addr = server.actual_tcp_addr;

        let captured_wire = Arc::new(Mutex::new(Vec::<u8>::new()));
        let captured_clone = captured_wire.clone();

        tokio::spawn(async move {
            let (mut client_sock, _) = listener.accept().await.unwrap();
            let mut server_sock = TcpStream::connect(server_addr).await.unwrap();
            let (mut cr, mut cw) = client_sock.split();
            let (mut sr, mut sw) = server_sock.split();

            let c2s = async {
                let mut buf = [0u8; 4096];
                loop {
                    let n = cr.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    captured_clone.lock().await.extend_from_slice(&buf[..n]);
                    if sw.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            };
            let s2c = async {
                let mut buf = [0u8; 4096];
                loop {
                    let n = sr.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    if cw.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            };
            tokio::join!(c2s, s2c);
        });

        let client = MeshNetwork::start(
            client_id.clone(),
            "client",
            hw.clone(),
            NetworkConfig::default(),
        )
        .await
        .unwrap();
        client.connect_peer(proxy_addr).await.unwrap();

        let _ = server.next_event().await.unwrap();
        let _ = client.next_event().await.unwrap();

        let secret_marker = "TOP_SECRET_MYCEL_WIRE_PAYLOAD_999";
        let task = TaskSpec::new("secret_task", TaskKind::BashScript, secret_marker);
        client
            .send_direct(
                server_id.peer_id(),
                MeshMessage::Rfq(mycel_proto::RfqMessage::new(
                    client_id.peer_id(),
                    task,
                    50,
                )),
            )
            .await
            .unwrap();

        let ev = server.next_event().await.unwrap();
        assert!(matches!(
            ev,
            MeshEvent::MessageReceived {
                message: MeshMessage::Rfq(_),
                ..
            }
        ));

        let wire = captured_wire.lock().await;
        assert!(
            !wire
                .windows(secret_marker.len())
                .any(|w| w == secret_marker.as_bytes()),
            "Plaintext payload must NOT appear on encrypted TCP wire"
        );
    }

    #[tokio::test]
    async fn test_wrong_signature_handshake_rejected() {
        let server_id = NodeIdentity::generate();
        let server = MeshNetwork::start(
            server_id.clone(),
            "server",
            HardwareProfile::default(),
            NetworkConfig::default(),
        )
        .await
        .unwrap();

        let mut stream = TcpStream::connect(server.actual_tcp_addr).await.unwrap();
        let spoof_id = NodeIdentity::generate();
        let ephem = EphemeralKeyPair::generate().unwrap();
        let mut forged_hs = SignedHandshake {
            peer_id: spoof_id.peer_id().to_string(),
            public_key_hex: spoof_id.public_key_hex(),
            ephemeral_pubkey: ephem.public_key_bytes,
            tcp_listen_port: 9000,
            heartbeat: PeerHeartbeat {
                peer_id: spoof_id.peer_id().to_string(),
                node_name: "spoof".into(),
                hardware: HardwareProfile::default(),
                timestamp: 0,
                uptime_secs: 0,
                active_tasks: 0,
            },
            signature: "00".repeat(64), // forged signature
        };
        let bytes = bincode::serialize(&forged_hs).unwrap();
        write_plain_frame(&mut stream, &bytes).await.unwrap();

        // Server should reject and close the stream without sending handshake back
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_millis(300), stream.read(&mut buf))
            .await
            .expect("Server should close stream promptly")
            .unwrap_or(0);
        assert_eq!(n, 0, "Expected server to reject forged handshake and close");

        // Also ensure mismatched peer_id/pubkey fails verify_remote_handshake directly
        forged_hs.signature = spoof_id.sign(&SignedHandshake::signing_payload(
            &forged_hs.peer_id,
            &forged_hs.public_key_hex,
            &forged_hs.ephemeral_pubkey,
            forged_hs.tcp_listen_port,
        ));
        forged_hs.peer_id = server_id.peer_id().to_string();
        forged_hs.heartbeat.peer_id = server_id.peer_id().to_string();
        assert!(verify_remote_handshake(&forged_hs, "other").is_err());
    }

    #[tokio::test]
    async fn test_tampered_ciphertext_frame_drops_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();

        let server_id = NodeIdentity::generate();
        let client_id = NodeIdentity::generate();
        let hw = HardwareProfile::default();

        let server = MeshNetwork::start(
            server_id.clone(),
            "server",
            hw.clone(),
            NetworkConfig::default(),
        )
        .await
        .unwrap();
        let server_addr = server.actual_tcp_addr;

        // Proxy flips a byte on the first encrypted application frame (after handshake)
        tokio::spawn(async move {
            let (mut client_sock, _) = listener.accept().await.unwrap();
            let mut server_sock = TcpStream::connect(server_addr).await.unwrap();
            let (mut cr, mut cw) = client_sock.split();
            let (mut sr, mut sw) = server_sock.split();

            let c2s = async {
                let mut frame_idx = 0usize;
                loop {
                    let mut len_buf = [0u8; 4];
                    if cr.read_exact(&mut len_buf).await.is_err() {
                        break;
                    }
                    let len = u32::from_be_bytes(len_buf) as usize;
                    let mut payload = vec![0u8; len];
                    if cr.read_exact(&mut payload).await.is_err() {
                        break;
                    }
                    if frame_idx >= 1 && !payload.is_empty() {
                        payload[0] ^= 0x55; // tamper encrypted frame
                    }
                    frame_idx += 1;
                    if sw.write_all(&len_buf).await.is_err() || sw.write_all(&payload).await.is_err()
                    {
                        break;
                    }
                }
            };
            let s2c = async {
                let mut buf = [0u8; 4096];
                loop {
                    let n = sr.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    if cw.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            };
            tokio::join!(c2s, s2c);
        });

        let client = MeshNetwork::start(
            client_id.clone(),
            "client",
            hw.clone(),
            NetworkConfig::default(),
        )
        .await
        .unwrap();
        client.connect_peer(proxy_addr).await.unwrap();

        assert!(matches!(
            server.next_event().await.unwrap(),
            MeshEvent::PeerDiscovered { .. }
        ));
        assert!(matches!(
            client.next_event().await.unwrap(),
            MeshEvent::PeerDiscovered { .. }
        ));

        client
            .send_direct(
                server_id.peer_id(),
                MeshMessage::Delta(CrdtDelta {
                    key: "k".into(),
                    value: b"v".to_vec(),
                    origin_peer_id: client_id.peer_id().into(),
                    lamport_clock: 1,
                    timestamp: 0,
                }),
            )
            .await
            .unwrap();

        // Server must reject tampered ciphertext and emit PeerLost instead of MessageReceived
        let ev = tokio::time::timeout(Duration::from_millis(500), server.next_event())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(ev, MeshEvent::PeerLost { .. }),
            "Expected PeerLost after tampered ciphertext, got {:?}",
            ev
        );
    }

    #[tokio::test]
    async fn test_udp_auto_discovery_connects_peers() {
        let id1 = NodeIdentity::generate();
        let id2 = NodeIdentity::generate();
        let hw = HardwareProfile::default();

        let discovery_port = 18977;
        let cfg1 = NetworkConfig {
            bind_ip: "127.0.0.1".into(),
            tcp_port: 0,
            discovery_port,
            heartbeat_interval: Duration::from_millis(100),
            enable_udp_broadcast: true,
        };
        let cfg2 = cfg1.clone();

        let net1 = MeshNetwork::start(id1.clone(), "udp_node_1", hw.clone(), cfg1)
            .await
            .unwrap();
        let net2 = MeshNetwork::start(id2.clone(), "udp_node_2", hw.clone(), cfg2)
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if net1.connected_peer_count().await >= 1 && net2.connected_peer_count().await >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out waiting for UDP auto-discovery"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Verify bidirectional encrypted messaging works without manual connect_peer
        net1.broadcast(MeshMessage::Delta(CrdtDelta {
            key: "udp_sync".into(),
            value: b"auto_discovered".to_vec(),
            origin_peer_id: id1.peer_id().into(),
            lamport_clock: 1,
            timestamp: 1,
        }))
        .await
        .unwrap();

        let mut got_msg = false;
        let msg_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < msg_deadline {
            if let Ok(Some(ev)) =
                tokio::time::timeout(Duration::from_millis(100), net2.next_event()).await
            {
                if let MeshEvent::MessageReceived {
                    message: MeshMessage::Delta(d),
                    ..
                } = ev
                {
                    if d.key == "udp_sync" && d.value == b"auto_discovered" {
                        got_msg = true;
                        break;
                    }
                }
            }
        }
        assert!(got_msg, "Expected auto-discovered peer to receive Delta");
    }
}
