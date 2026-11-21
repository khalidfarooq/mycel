use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, Mutex, RwLock};
use thiserror::Error;
use tracing::info;

use mycel_crypto::NodeIdentity;
use mycel_proto::{HardwareProfile, MeshMessage, PeerHeartbeat};

#[derive(Error, Debug)]
pub enum NetError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Bincode(#[from] bincode::Error),
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
            heartbeat_interval: Duration::from_millis(1000),
            enable_udp_broadcast: false, // Default off in unit test environments
        }
    }
}

pub struct MeshNetwork {
    pub identity: NodeIdentity,
    pub node_name: String,
    pub hardware: HardwareProfile,
    pub actual_tcp_addr: SocketAddr,
    pub config: NetworkConfig,
    event_tx: mpsc::Sender<MeshEvent>,
    event_rx: Arc<Mutex<mpsc::Receiver<MeshEvent>>>,
    active_connections: Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>>,
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
        let active_connections: Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let peers: Arc<RwLock<HashMap<String, SocketAddr>>> = Arc::new(RwLock::new(HashMap::new()));

        // Inbound TCP acceptor loop
        let conn_map = active_connections.clone();
        let ev_tx = event_tx.clone();
        let my_id = identity.clone();
        let my_name = node_name.clone();
        let my_hw = hardware.clone();

        tokio::spawn(async move {
            while let Ok((stream, _)) = tcp_listener.accept().await {
                let conn_map_clone = conn_map.clone();
                let ev_tx_clone = ev_tx.clone();
                let my_id_clone = my_id.clone();
                let my_name_clone = my_name.clone();
                let my_hw_clone = my_hw.clone();

                tokio::spawn(async move {
                    Self::handle_inbound_stream(
                        stream,
                        conn_map_clone,
                        ev_tx_clone,
                        my_id_clone,
                        my_name_clone,
                        my_hw_clone,
                    )
                    .await;
                });
            }
        });

        // Optional UDP Discovery Beacon
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
            event_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            active_connections,
            peers,
        })
    }

    /// Connect directly to a peer at a known TCP address (outbound client handshake)
    pub async fn connect_peer(&self, addr: SocketAddr) -> Result<(), NetError> {
        let stream = TcpStream::connect(addr).await?;
        let (mut reader, mut writer) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100);

        // 1. Send our handshake
        let my_hb = PeerHeartbeat {
            peer_id: self.identity.peer_id().to_string(),
            node_name: self.node_name.clone(),
            hardware: self.hardware.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            uptime_secs: 0,
            active_tasks: 0,
        };
        let init_bytes = MeshMessage::Heartbeat(my_hb).to_bytes()?;
        let len = (init_bytes.len() as u32).to_be_bytes();
        writer.write_all(&len).await?;
        writer.write_all(&init_bytes).await?;

        // 2. Read server's handshake
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; msg_len];
        reader.read_exact(&mut buf).await?;

        let remote_msg = MeshMessage::from_bytes(&buf)?;
        let remote_peer_id = match remote_msg {
            MeshMessage::Heartbeat(hb) => {
                let pid = hb.peer_id.clone();
                let _ = self
                    .event_tx
                    .send(MeshEvent::PeerDiscovered {
                        peer: hb,
                        addr,
                    })
                    .await;
                pid
            }
            _ => return Err(NetError::HandshakeFailed("Expected heartbeat in handshake".into())),
        };

        // Register writer in active connections
        self.active_connections
            .lock()
            .await
            .insert(remote_peer_id.clone(), tx);
        self.peers.write().await.insert(remote_peer_id.clone(), addr);

        // Background writer loop
        tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                let len = (bytes.len() as u32).to_be_bytes();
                if writer.write_all(&len).await.is_err() || writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });

        // Background reader loop
        let ev_tx = self.event_tx.clone();
        let conn_map = self.active_connections.clone();
        let pid_clone = remote_peer_id.clone();

        tokio::spawn(async move {
            let mut len_buf = [0u8; 4];
            while reader.read_exact(&mut len_buf).await.is_ok() {
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; msg_len];
                if reader.read_exact(&mut buf).await.is_ok() {
                    if let Ok(msg) = MeshMessage::from_bytes(&buf) {
                        let _ = ev_tx
                            .send(MeshEvent::MessageReceived {
                                from_peer_id: pid_clone.clone(),
                                message: msg,
                            })
                            .await;
                    }
                }
            }
            conn_map.lock().await.remove(&pid_clone);
            let _ = ev_tx.send(MeshEvent::PeerLost { peer_id: pid_clone }).await;
        });

        Ok(())
    }

    async fn handle_inbound_stream(
        stream: TcpStream,
        conn_map: Arc<Mutex<HashMap<String, mpsc::Sender<Vec<u8>>>>>,
        event_tx: mpsc::Sender<MeshEvent>,
        my_id: NodeIdentity,
        my_name: String,
        my_hw: HardwareProfile,
    ) {
        let (mut reader, mut writer) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(100);

        // 1. Read client's handshake
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).await.is_err() {
            return;
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; msg_len];
        if reader.read_exact(&mut buf).await.is_err() {
            return;
        }

        let client_msg = match MeshMessage::from_bytes(&buf) {
            Ok(m) => m,
            Err(_) => return,
        };

        let client_peer_id = match client_msg {
            MeshMessage::Heartbeat(hb) => {
                let pid = hb.peer_id.clone();
                let _ = event_tx
                    .send(MeshEvent::PeerDiscovered {
                        peer: hb,
                        addr: "127.0.0.1:0".parse().unwrap(),
                    })
                    .await;
                pid
            }
            _ => return,
        };

        // 2. Send our server handshake back
        let my_hb = PeerHeartbeat {
            peer_id: my_id.peer_id().to_string(),
            node_name: my_name,
            hardware: my_hw,
            timestamp: chrono::Utc::now().timestamp_millis(),
            uptime_secs: 0,
            active_tasks: 0,
        };
        if let Ok(init_bytes) = MeshMessage::Heartbeat(my_hb).to_bytes() {
            let len = (init_bytes.len() as u32).to_be_bytes();
            if writer.write_all(&len).await.is_err() || writer.write_all(&init_bytes).await.is_err() {
                return;
            }
        }

        // Register in connection map
        conn_map.lock().await.insert(client_peer_id.clone(), tx);

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
        let pid_clone = client_peer_id.clone();
        tokio::spawn(async move {
            let mut len_buf = [0u8; 4];
            while reader.read_exact(&mut len_buf).await.is_ok() {
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut buf = vec![0u8; msg_len];
                if reader.read_exact(&mut buf).await.is_ok() {
                    if let Ok(msg) = MeshMessage::from_bytes(&buf) {
                        let _ = event_tx
                            .send(MeshEvent::MessageReceived {
                                from_peer_id: pid_clone.clone(),
                                message: msg,
                            })
                            .await;
                    }
                }
            }
            conn_map.lock().await.remove(&pid_clone);
            let _ = event_tx.send(MeshEvent::PeerLost { peer_id: pid_clone }).await;
        });
    }

    /// Broadcast message to all connected peers in mesh
    pub async fn broadcast(&self, message: MeshMessage) -> Result<(), NetError> {
        let bytes = message.to_bytes()?;
        let map = self.active_connections.lock().await;
        for tx in map.values() {
            let _ = tx.send(bytes.clone()).await;
        }
        Ok(())
    }

    /// Send message directly to a target peer
    pub async fn send_direct(&self, target_peer_id: &str, message: MeshMessage) -> Result<(), NetError> {
        let bytes = message.to_bytes()?;
        let map = self.active_connections.lock().await;
        if let Some(tx) = map.get(target_peer_id) {
            let _ = tx.send(bytes).await;
        }
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
    async fn test_mesh_network_mutual_handshake() {
        let id1 = NodeIdentity::generate();
        let id2 = NodeIdentity::generate();

        let hw = HardwareProfile::default();
        let cfg1 = NetworkConfig::default();
        let cfg2 = NetworkConfig::default();

        let net1 = MeshNetwork::start(id1.clone(), "node_alpha", hw.clone(), cfg1).await.unwrap();
        let net2 = MeshNetwork::start(id2.clone(), "node_beta", hw.clone(), cfg2).await.unwrap();

        // Connect net2 to net1
        net2.connect_peer(net1.actual_tcp_addr).await.expect("Connect peer should succeed");

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Net1 receives Net2 discovery
        let ev1 = net1.next_event().await.unwrap();
        match ev1 {
            MeshEvent::PeerDiscovered { peer, .. } => {
                assert_eq!(peer.peer_id, id2.peer_id());
            }
            _ => panic!("Expected Net1 to discover Net2"),
        }

        // Net2 receives Net1 discovery
        let ev2 = net2.next_event().await.unwrap();
        match ev2 {
            MeshEvent::PeerDiscovered { peer, .. } => {
                assert_eq!(peer.peer_id, id1.peer_id());
            }
            _ => panic!("Expected Net2 to discover Net1"),
        }

        // Test direct message sending
        net1.send_direct(id2.peer_id(), MeshMessage::Heartbeat(PeerHeartbeat {
            peer_id: id1.peer_id().to_string(),
            node_name: "node_alpha".into(),
            hardware: hw.clone(),
            timestamp: 100,
            uptime_secs: 10,
            active_tasks: 0,
        })).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let ev_msg = net2.next_event().await.unwrap();
        match ev_msg {
            MeshEvent::MessageReceived { from_peer_id, .. } => {
                assert_eq!(from_peer_id, id1.peer_id());
            }
            _ => panic!("Expected MessageReceived on Net2"),
        }
    }
}
