use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex, RwLock};
use thiserror::Error;
use tracing::{debug, error, info};

use mycel_crypto::NodeIdentity;
use mycel_proto::{HardwareProfile, MeshMessage, PeerHeartbeat};

#[derive(Error, Debug)]
pub enum NetError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Peer not found: {0}")]
    PeerNotFound(String),
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
            tcp_port: 0, // OS assigns port
            discovery_port: 8766,
            heartbeat_interval: Duration::from_millis(1000),
            enable_udp_broadcast: true,
        }
    }
}

pub struct MeshNetwork {
    pub identity: NodeIdentity,
    pub node_name: String,
    pub hardware: HardwareProfile,
    pub actual_tcp_addr: SocketAddr,
    pub config: NetworkConfig,
    outbound_tx: mpsc::Sender<(String, MeshMessage)>,
    broadcast_tx: mpsc::Sender<MeshMessage>,
    event_rx: Arc<Mutex<mpsc::Receiver<MeshEvent>>>,
    pub peers: Arc<RwLock<HashMap<String, SocketAddr>>>,
}

impl MeshNetwork {
    pub async fn start(
        identity: NodeIdentity,
        node_name: impl Into<String>,
        hardware: HardwareProfile,
        config: NetworkConfig,
    ) -> Result<Self, NetError> {
        let node_name = node_name.into();
        let tcp_listener = TcpListener::bind(format!("{}:{}", config.bind_ip, config.tcp_port)).await?;
        let actual_tcp_addr = tcp_listener.local_addr()?;
        info!(addr = %actual_tcp_addr, node = %node_name, "Mycel TCP listener bound");

        let (event_tx, event_rx) = mpsc::channel::<MeshEvent>(1000);
        let (broadcast_tx, mut broadcast_rx) = mpsc::channel::<MeshMessage>(1000);
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<(String, MeshMessage)>(1000);

        let peers: Arc<RwLock<HashMap<String, SocketAddr>>> = Arc::new(RwLock::new(HashMap::new()));
        let active_connections: Arc<Mutex<HashMap<String, tokio::sync::mpsc::Sender<Vec<u8>>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Inbound TCP acceptor loop
        let conn_map = active_connections.clone();
        let ev_tx = event_tx.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = tcp_listener.accept().await {
                let conn_map_clone = conn_map.clone();
                let ev_tx_clone = ev_tx.clone();
                tokio::spawn(async move {
                    Self::handle_inbound_stream(stream, conn_map_clone, ev_tx_clone).await;
                });
            }
        });

        // Broadcast handler
        let conn_map_bcast = active_connections.clone();
        tokio::spawn(async move {
            while let Some(msg) = broadcast_rx.recv().await {
                if let Ok(bytes) = msg.to_bytes() {
                    let map = conn_map_bcast.lock().await;
                    for tx in map.values() {
                        let _ = tx.send(bytes.clone()).await;
                    }
                }
            }
        });

        // Direct outbound sender
        let conn_map_direct = active_connections.clone();
        tokio::spawn(async move {
            while let Some((peer_id, msg)) = outbound_rx.recv().await {
                if let Ok(bytes) = msg.to_bytes() {
                    let map = conn_map_direct.lock().await;
                    if let Some(tx) = map.get(&peer_id) {
                        let _ = tx.send(bytes).await;
                    }
                }
            }
        });

        // UDP Discovery Beacon (Heartbeats)
        if config.enable_udp_broadcast {
            let udp_bind = format!("{}:0", config.bind_ip);
            if let Ok(udp_sock) = UdpSocket::bind(&udp_bind).await {
                let _ = udp_sock.set_broadcast(true);
                let peer_id = identity.peer_id().to_string();
                let name = node_name.clone();
                let hw = hardware.clone();
                let target = format!("{}:{}", config.bind_ip, config.discovery_port);
                let interval = config.heartbeat_interval;

                tokio::spawn(async move {
                    let mut uptime = 0u64;
                    loop {
                        let hb = PeerHeartbeat {
                            peer_id: peer_id.clone(),
                            node_name: name.clone(),
                            hardware: hw.clone(),
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            uptime_secs: uptime,
                            active_tasks: 0,
                        };
                        let msg = MeshMessage::Heartbeat(hb);
                        if let Ok(bytes) = msg.to_bytes() {
                            let _ = udp_sock.send_to(&bytes, &target).await;
                        }
                        tokio::time::sleep(interval).await;
                        uptime += interval.as_secs();
                    }
                });
            }
        }

        Ok(Self {
            identity,
            node_name,
            hardware,
            actual_tcp_addr,
            config,
            outbound_tx,
            broadcast_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            peers,
        })
    }

    /// Connect directly to a peer at a known TCP address
    pub async fn connect_peer(&self, addr: SocketAddr) -> Result<(), NetError> {
        let stream = TcpStream::connect(addr).await?;
        let (mut reader, mut writer) = stream.into_split();

        // Send our initial handshake heartbeat
        let hb = PeerHeartbeat {
            peer_id: self.identity.peer_id().to_string(),
            node_name: self.node_name.clone(),
            hardware: self.hardware.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            uptime_secs: 0,
            active_tasks: 0,
        };
        let init_bytes = MeshMessage::Heartbeat(hb).to_bytes()?;
        let len = (init_bytes.len() as u32).to_be_bytes();
        writer.write_all(&len).await?;
        writer.write_all(&init_bytes).await?;

        // Background reader loop
        let ev_tx = self.event_rx.clone();
        tokio::spawn(async move {
            let mut len_buf = [0u8; 4];
            while reader.read_exact(&mut len_buf).await.is_ok() {
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; msg_len];
                if reader.read_exact(&mut buf).await.is_ok() {
                    if let Ok(msg) = MeshMessage::from_bytes(&buf) {
                        debug!("Received direct TCP message");
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_inbound_stream(
        stream: TcpStream,
        conn_map: Arc<Mutex<HashMap<String, tokio::sync::mpsc::Sender<Vec<u8>>>>>,
        event_tx: mpsc::Sender<MeshEvent>,
    ) {
        let (mut reader, mut writer) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100);

        // Writer task
        tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                let len = (bytes.len() as u32).to_be_bytes();
                if writer.write_all(&len).await.is_err() || writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });

        // Reader task
        let mut peer_id_opt: Option<String> = None;
        let mut len_buf = [0u8; 4];

        while reader.read_exact(&mut len_buf).await.is_ok() {
            let msg_len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; msg_len];
            if reader.read_exact(&mut buf).await.is_ok() {
                if let Ok(msg) = MeshMessage::from_bytes(&buf) {
                    match &msg {
                        MeshMessage::Heartbeat(hb) => {
                            if peer_id_opt.is_none() {
                                peer_id_opt = Some(hb.peer_id.clone());
                                conn_map.lock().await.insert(hb.peer_id.clone(), tx.clone());
                                let _ = event_tx
                                    .send(MeshEvent::PeerDiscovered {
                                        peer: hb.clone(),
                                        addr: "127.0.0.1:0".parse().unwrap(),
                                    })
                                    .await;
                            }
                        }
                        _ => {}
                    }
                    if let Some(ref from) = peer_id_opt {
                        let _ = event_tx
                            .send(MeshEvent::MessageReceived {
                                from_peer_id: from.clone(),
                                message: msg,
                            })
                            .await;
                    }
                }
            }
        }

        if let Some(peer_id) = peer_id_opt {
            conn_map.lock().await.remove(&peer_id);
            let _ = event_tx.send(MeshEvent::PeerLost { peer_id }).await;
        }
    }

    /// Broadcast message to all connected peers in mesh
    pub async fn broadcast(&self, message: MeshMessage) -> Result<(), NetError> {
        self.broadcast_tx
            .send(message)
            .await
            .map_err(|_| NetError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Broadcast channel closed")))?;
        Ok(())
    }

    /// Send message directly to a target peer
    pub async fn send_direct(&self, target_peer_id: &str, message: MeshMessage) -> Result<(), NetError> {
        self.outbound_tx
            .send((target_peer_id.to_string(), message))
            .await
            .map_err(|_| NetError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Outbound channel closed")))?;
        Ok(())
    }

    /// Fetch next network event
    pub async fn next_event(&self) -> Option<MeshEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mesh_network_direct_connect() {
        let id1 = NodeIdentity::generate();
        let id2 = NodeIdentity::generate();

        let hw = HardwareProfile::default();
        let cfg1 = NetworkConfig::default();
        let cfg2 = NetworkConfig::default();

        let net1 = MeshNetwork::start(id1.clone(), "node_alpha", hw.clone(), cfg1).await.unwrap();
        let net2 = MeshNetwork::start(id2.clone(), "node_beta", hw.clone(), cfg2).await.unwrap();

        // Connect net2 to net1
        net2.connect_peer(net1.actual_tcp_addr).await.expect("Connect peer should succeed");

        // Wait briefly for handshake
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Net1 should have received discovery event from Net2
        if let Some(event) = net1.next_event().await {
            match event {
                MeshEvent::PeerDiscovered { peer, .. } => {
                    assert_eq!(peer.peer_id, id2.peer_id());
                    assert_eq!(peer.node_name, "node_beta");
                }
                _ => panic!("Expected PeerDiscovered event"),
            }
        }
    }
}
