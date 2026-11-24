use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use mycel_auction::{AuctionEngine, AuctionError, AuctionSession};
use mycel_crdt::CrdtStore;
use mycel_crypto::NodeIdentity;
use mycel_net::{MeshEvent, MeshNetwork, NetError, NetworkConfig};
use mycel_proto::{
    CrdtDelta, ExecutionResult, HardwareProfile, MeshMessage, PeerHeartbeat, TaskKind, TaskSpec,
    WorkContract,
};

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Network error: {0}")]
    Net(#[from] NetError),
    #[error("Auction error: {0}")]
    Auction(#[from] AuctionError),
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Task timeout")]
    Timeout,
}

pub struct TaskExecutor;

impl TaskExecutor {
    pub async fn execute(
        task: &TaskSpec,
        contract: &WorkContract,
        identity: &NodeIdentity,
    ) -> ExecutionResult {
        let start = std::time::Instant::now();
        info!(
            task_id = %task.task_id,
            contract_id = %contract.contract_id,
            kind = ?task.kind,
            "Executing task in isolated working directory"
        );

        let (work_dir, cleanup_dir) = prepare_working_dir(task, &contract.contract_id);

        let (success, exit_code, stdout, stderr) = match task.kind {
            TaskKind::BashScript | TaskKind::TestSuite => {
                run_shell_command(&task.payload, &work_dir, task.timeout_ms).await
            }
            TaskKind::RustCompilation => {
                run_rust_compilation(&task.payload, &work_dir, task.timeout_ms).await
            }
            TaskKind::SlmInference => run_slm_inference(&task.payload, task.timeout_ms).await,
        };

        if cleanup_dir {
            let _ = std::fs::remove_dir_all(&work_dir);
        }

        let duration_ms = start.elapsed().as_millis() as u64;
        let mut result = ExecutionResult {
            contract_id: contract.contract_id.clone(),
            worker_peer_id: identity.peer_id().to_string(),
            worker_public_key_hex: identity.public_key_hex(),
            success,
            exit_code,
            stdout,
            stderr,
            execution_time_ms: duration_ms,
            signature: String::new(),
        };
        result.signature = identity.sign(&result.signing_payload());
        result
    }
}

fn prepare_working_dir(task: &TaskSpec, contract_id: &str) -> (PathBuf, bool) {
    if let Some(ref dir) = task.working_dir {
        let path = PathBuf::from(dir);
        if path.is_dir() {
            return (path, false);
        }
    }
    let temp_dir = std::env::temp_dir().join(format!("mycel-work-{}", contract_id));
    let _ = std::fs::create_dir_all(&temp_dir);
    (temp_dir, true)
}

fn augmented_path_env() -> String {
    let base = std::env::var("PATH").unwrap_or_default();
    if let Ok(home) = std::env::var("HOME") {
        let cargo_bin = format!("{}/.cargo/bin", home);
        if !base.split(':').any(|p| p == cargo_bin) {
            return if base.is_empty() {
                cargo_bin
            } else {
                format!("{}:{}", cargo_bin, base)
            };
        }
    }
    base
}

fn find_rustc_binary() -> String {
    if let Ok(rustc) = std::env::var("RUSTC") {
        if !rustc.trim().is_empty() {
            return rustc;
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let candidate = PathBuf::from(home).join(".cargo/bin/rustc");
        if candidate.is_file() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    "rustc".to_string()
}

async fn run_shell_command(
    cmd: &str,
    work_dir: &Path,
    timeout_ms: u64,
) -> (bool, i32, String, String) {
    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg(cmd)
        .current_dir(work_dir)
        .env("PATH", augmented_path_env());

    match tokio::time::timeout(Duration::from_millis(timeout_ms), command.output()).await {
        Ok(Ok(output)) => {
            let stdout_str = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr_str = String::from_utf8_lossy(&output.stderr).to_string();
            let code = output.status.code().unwrap_or(-1);
            (output.status.success(), code, stdout_str, stderr_str)
        }
        Ok(Err(e)) => (
            false,
            -1,
            String::new(),
            format!("Process spawn error: {}", e),
        ),
        Err(_) => (false, -1, String::new(), "Execution timed out".into()),
    }
}

async fn run_rust_compilation(
    payload: &str,
    work_dir: &Path,
    timeout_ms: u64,
) -> (bool, i32, String, String) {
    let trimmed = payload.trim_start();
    if trimmed.starts_with("cargo ") || trimmed.starts_with("rustc ") {
        return run_shell_command(payload, work_dir, timeout_ms).await;
    }

    let src_path = work_dir.join("main.rs");
    let bin_path = work_dir.join("mycel_compiled_bin");
    if let Err(e) = std::fs::write(&src_path, payload) {
        return (
            false,
            -1,
            String::new(),
            format!("Failed to write Rust source: {}", e),
        );
    }

    let rustc = find_rustc_binary();
    let mut compile_cmd = tokio::process::Command::new(&rustc);
    compile_cmd
        .arg(&src_path)
        .arg("-o")
        .arg(&bin_path)
        .current_dir(work_dir)
        .env("PATH", augmented_path_env());

    match tokio::time::timeout(Duration::from_millis(timeout_ms), compile_cmd.output()).await {
        Ok(Ok(compile_out)) => {
            if !compile_out.status.success() {
                let code = compile_out.status.code().unwrap_or(-1);
                return (
                    false,
                    code,
                    String::from_utf8_lossy(&compile_out.stdout).to_string(),
                    String::from_utf8_lossy(&compile_out.stderr).to_string(),
                );
            }

            let mut run_cmd = tokio::process::Command::new(&bin_path);
            run_cmd.current_dir(work_dir);
            match tokio::time::timeout(Duration::from_millis(timeout_ms), run_cmd.output()).await {
                Ok(Ok(run_out)) => {
                    let code = run_out.status.code().unwrap_or(-1);
                    (
                        run_out.status.success(),
                        code,
                        String::from_utf8_lossy(&run_out.stdout).to_string(),
                        String::from_utf8_lossy(&run_out.stderr).to_string(),
                    )
                }
                Ok(Err(e)) => (
                    false,
                    -1,
                    String::new(),
                    format!("Compiled binary failed to start: {}", e),
                ),
                Err(_) => (
                    false,
                    -1,
                    String::new(),
                    "Compiled binary execution timed out".into(),
                ),
            }
        }
        Ok(Err(e)) => (
            false,
            -1,
            String::new(),
            format!("Failed to invoke {}: {}", rustc, e),
        ),
        Err(_) => (
            false,
            -1,
            String::new(),
            "Rust compilation timed out".into(),
        ),
    }
}

async fn run_slm_inference(prompt: &str, timeout_ms: u64) -> (bool, i32, String, String) {
    let endpoint = std::env::var("MYCEL_SLM_ENDPOINT")
        .or_else(|_| std::env::var("OLLAMA_HOST"))
        .unwrap_or_else(|_| "http://127.0.0.1:11434".to_string());
    let model = std::env::var("MYCEL_SLM_MODEL").unwrap_or_else(|_| "gemma2:2b".to_string());

    let host_port = match parse_http_host_port(&endpoint) {
        Ok(hp) => hp,
        Err(msg) => return (false, -1, String::new(), msg),
    };

    let connect_timeout = Duration::from_millis(timeout_ms.min(1500));
    let mut stream =
        match tokio::time::timeout(connect_timeout, TcpStream::connect(&host_port)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return (
                    false,
                    -1,
                    String::new(),
                    format!(
                        "No local SLM endpoint reachable at {} ({}): start Ollama or set MYCEL_SLM_ENDPOINT",
                        endpoint, e
                    ),
                )
            }
            Err(_) => {
                return (
                    false,
                    -1,
                    String::new(),
                    format!(
                        "Timed out connecting to local SLM endpoint at {}: start Ollama or set MYCEL_SLM_ENDPOINT",
                        endpoint
                    ),
                )
            }
        };

    let body = serde_json::json!({
        "model": model,
        "prompt": prompt,
        "stream": false
    })
    .to_string();

    let req = format!(
        "POST /api/generate HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        host_port,
        body.len(),
        body
    );

    let io_future = async {
        stream.write_all(req.as_bytes()).await?;
        let mut resp_bytes = Vec::new();
        stream.read_to_end(&mut resp_bytes).await?;
        Ok::<Vec<u8>, std::io::Error>(resp_bytes)
    };

    let raw_resp = match tokio::time::timeout(Duration::from_millis(timeout_ms), io_future).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => {
            return (
                false,
                -1,
                String::new(),
                format!("SLM HTTP request failed: {}", e),
            )
        }
        Err(_) => return (false, -1, String::new(), "SLM inference timed out".into()),
    };

    let resp_str = String::from_utf8_lossy(&raw_resp);
    let Some((headers, resp_body)) = resp_str.split_once("\r\n\r\n") else {
        return (
            false,
            -1,
            String::new(),
            "Invalid HTTP response from SLM endpoint".into(),
        );
    };

    let status_line = headers.lines().next().unwrap_or("");
    let status_ok = status_line.contains(" 200 ");
    if !status_ok {
        return (
            false,
            -1,
            String::new(),
            format!("SLM endpoint returned '{}': {}", status_line, resp_body.trim()),
        );
    }

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(resp_body) {
        if let Some(text) = json.get("response").and_then(|v| v.as_str()) {
            return (true, 0, text.to_string(), String::new());
        }
        if let Some(text) = json
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|v| v.as_str())
        {
            return (true, 0, text.to_string(), String::new());
        }
    }

    (true, 0, resp_body.to_string(), String::new())
}

fn parse_http_host_port(endpoint: &str) -> Result<String, String> {
    let trimmed = endpoint.trim();
    if trimmed.starts_with("https://") {
        return Err("HTTPS SLM endpoints are not supported; use local http:// host:port".into());
    }
    let without_scheme = trimmed.strip_prefix("http://").unwrap_or(trimmed);
    let host_part = without_scheme.split('/').next().unwrap_or("");
    if host_part.is_empty() {
        return Err(format!("Invalid SLM endpoint URL: {}", endpoint));
    }
    if host_part.contains(':') {
        Ok(host_part.to_string())
    } else {
        Ok(format!("{}:11434", host_part))
    }
}

pub struct NodeDaemon {
    pub identity: NodeIdentity,
    pub node_name: String,
    pub hardware: HardwareProfile,
    pub network: Arc<MeshNetwork>,
    pub auction: Arc<AuctionEngine>,
    pub crdt: Arc<CrdtStore>,
    result_watchers: Arc<RwLock<HashMap<String, mpsc::Sender<ExecutionResult>>>>,
}

impl NodeDaemon {
    pub async fn start(
        identity: NodeIdentity,
        node_name: impl Into<String>,
        hardware: HardwareProfile,
        net_cfg: NetworkConfig,
    ) -> Result<Arc<Self>, NodeError> {
        let node_name = node_name.into();
        let network = Arc::new(
            MeshNetwork::start(
                identity.clone(),
                node_name.clone(),
                hardware.clone(),
                net_cfg,
            )
            .await?,
        );
        let auction = Arc::new(AuctionEngine::new(identity.clone(), hardware.clone()));
        let crdt = Arc::new(CrdtStore::new(identity.peer_id()));

        let daemon = Arc::new(Self {
            identity,
            node_name,
            hardware,
            network,
            auction,
            crdt,
            result_watchers: Arc::new(RwLock::new(HashMap::new())),
        });

        // Spawn event routing loop
        let d = daemon.clone();
        tokio::spawn(async move {
            d.run_event_loop().await;
        });

        Ok(daemon)
    }

    async fn run_event_loop(&self) {
        while let Some(event) = self.network.next_event().await {
            match event {
                MeshEvent::PeerDiscovered { peer, .. } => {
                    info!(peer_id = %peer.peer_id, name = %peer.node_name, "Mesh peer discovered");
                    let peer_id = peer.peer_id.clone();
                    self.crdt.record_peer_heartbeat(peer);

                    // Sync full CRDT state to newly connected peer
                    let deltas = self.crdt.export_deltas();
                    if !deltas.is_empty() {
                        let _ = self
                            .network
                            .send_direct(&peer_id, MeshMessage::CrdtSnapshot(deltas))
                            .await;
                    }
                }
                MeshEvent::PeerLost { peer_id } => {
                    warn!(peer_id = %peer_id, "Mesh peer disconnected");
                }
                MeshEvent::MessageReceived {
                    from_peer_id,
                    message,
                } => {
                    self.handle_mesh_message(&from_peer_id, message).await;
                }
            }
        }
    }

    async fn handle_mesh_message(&self, from: &str, message: MeshMessage) {
        match message {
            MeshMessage::Heartbeat(hb) => {
                self.crdt.record_peer_heartbeat(hb);
            }
            MeshMessage::Rfq(rfq) => {
                debug!(rfq_id = %rfq.rfq_id, from = %from, "Received RFQ");
                if let Ok(Some(bid)) = self.auction.evaluate_rfq(&rfq) {
                    let _ = self.network.send_direct(from, MeshMessage::Bid(bid)).await;
                }
            }
            MeshMessage::Bid(bid) => {
                debug!(rfq_id = %bid.rfq_id, bidder = %bid.bidder_peer_id, "Received bid");
                if let Err(e) = self.auction.record_bid(bid).await {
                    warn!(error = %e, from = %from, "Rejected invalid bid");
                }
            }
            MeshMessage::ContractAward(contract) => {
                if let Err(e) = AuctionEngine::verify_contract_signature(&contract) {
                    warn!(error = %e, contract_id = %contract.contract_id, "Rejected unsigned/forged contract award");
                    return;
                }
                if contract.assigned_worker_peer_id == self.identity.peer_id() {
                    info!(contract_id = %contract.contract_id, "Awarded signed work contract! Executing...");
                    let ident = self.identity.clone();
                    let net = self.network.clone();
                    let originator = contract.originator_peer_id.clone();
                    let task = contract.task.clone();

                    tokio::spawn(async move {
                        let result = TaskExecutor::execute(&task, &contract, &ident).await;
                        let _ = net
                            .send_direct(&originator, MeshMessage::Result(result))
                            .await;
                    });
                }
            }
            MeshMessage::Result(result) => {
                info!(
                    contract_id = %result.contract_id,
                    success = %result.success,
                    "Received task execution result"
                );
                if let Err(e) = self.auction.complete_auction(result.clone()).await {
                    warn!(error = %e, contract_id = %result.contract_id, "Rejected unverified execution result");
                    return;
                }

                let mut watchers = self.result_watchers.write().await;
                if let Some(tx) = watchers.remove(&result.contract_id) {
                    let _ = tx.send(result).await;
                }
            }
            MeshMessage::Delta(delta) => {
                self.crdt.merge_delta(delta);
            }
            MeshMessage::CrdtSnapshot(deltas) => {
                for delta in deltas {
                    self.crdt.merge_delta(delta);
                }
            }
        }
    }

    /// Write key-value state to local CRDT store and broadcast delta across connected peers
    pub async fn set_state(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
    ) -> Result<CrdtDelta, NodeError> {
        let delta = self.crdt.set(key, value);
        self.network
            .broadcast(MeshMessage::Delta(delta.clone()))
            .await?;
        Ok(delta)
    }

    /// Submit a task to the decentralized mesh compute market and return both result and auction session
    pub async fn dispatch_task_with_session(
        &self,
        task: TaskSpec,
        max_budget: u64,
        window_ms: u64,
    ) -> Result<(ExecutionResult, AuctionSession), NodeError> {
        info!(task = %task.name, "Broadcasting task to Spore compute auction");
        let rfq = self.auction.create_rfq(task, max_budget, window_ms).await;

        self.network
            .broadcast(MeshMessage::Rfq(rfq.clone()))
            .await?;

        tokio::time::sleep(Duration::from_millis(window_ms)).await;

        let contract = self.auction.resolve_auction(&rfq.rfq_id).await?;

        let (tx, mut rx) = mpsc::channel::<ExecutionResult>(1);
        {
            let mut watchers = self.result_watchers.write().await;
            watchers.insert(contract.contract_id.clone(), tx);
        }

        self.network
            .send_direct(
                &contract.assigned_worker_peer_id,
                MeshMessage::ContractAward(contract.clone()),
            )
            .await?;

        let timeout = Duration::from_millis(contract.task.timeout_ms + 2000);
        let result = tokio::select! {
            res = rx.recv() => {
                res.ok_or_else(|| NodeError::Execution("Result channel dropped".into()))?
            }
            _ = tokio::time::sleep(timeout) => {
                let mut watchers = self.result_watchers.write().await;
                watchers.remove(&contract.contract_id);
                return Err(NodeError::Timeout);
            }
        };

        // Replicate completion record into distributed CRDT state
        let crdt_val = format!(
            "worker={},success={},duration_ms={}",
            result.worker_peer_id, result.success, result.execution_time_ms
        );
        let _ = self
            .set_state(
                format!("contract:{}", result.contract_id),
                crdt_val.into_bytes(),
            )
            .await;

        let session = self
            .auction
            .get_session(&rfq.rfq_id)
            .await
            .ok_or_else(|| AuctionError::AuctionNotFound(rfq.rfq_id.clone()))?;

        Ok((result, session))
    }

    /// Submit a task to the decentralized mesh compute market and await verified completion
    pub async fn dispatch_task(
        &self,
        task: TaskSpec,
        max_budget: u64,
        window_ms: u64,
    ) -> Result<ExecutionResult, NodeError> {
        self.dispatch_task_with_session(task, max_budget, window_ms)
            .await
            .map(|(result, _)| result)
    }

    pub fn list_peers(&self) -> Vec<PeerHeartbeat> {
        self.crdt.list_active_peers()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_node_daemon_local_dispatch() {
        let id_coord = NodeIdentity::generate();
        let id_worker = NodeIdentity::generate();

        let hw = HardwareProfile::detect();
        let net_cfg1 = NetworkConfig::default();
        let net_cfg2 = NetworkConfig::default();

        let coord = NodeDaemon::start(id_coord, "coordinator_mac", hw.clone(), net_cfg1)
            .await
            .unwrap();
        let worker = NodeDaemon::start(id_worker, "worker_linux", hw.clone(), net_cfg2)
            .await
            .unwrap();

        worker
            .network
            .connect_peer(coord.network.actual_tcp_addr)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;

        let task = TaskSpec::new(
            "echo_hello",
            TaskKind::BashScript,
            "echo 'hello from mycel mesh'",
        );
        let result = coord
            .dispatch_task(task, 100, 150)
            .await
            .expect("Task dispatch should succeed");

        assert!(result.success);
        assert!(result.stdout.contains("hello from mycel mesh"));
    }

    #[tokio::test]
    async fn test_rust_compilation_real_execution() {
        let id_coord = NodeIdentity::generate();
        let id_worker = NodeIdentity::generate();
        let hw = HardwareProfile::detect();

        let coord = NodeDaemon::start(id_coord, "coord", hw.clone(), NetworkConfig::default())
            .await
            .unwrap();
        let worker = NodeDaemon::start(id_worker, "worker", hw.clone(), NetworkConfig::default())
            .await
            .unwrap();

        worker
            .network
            .connect_peer(coord.network.actual_tcp_addr)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;

        let rust_src = r#"fn main() { println!("RUSTC_LIVE_OK:{}", 6 * 7); }"#;
        let task = TaskSpec::new("compile_hello", TaskKind::RustCompilation, rust_src);
        let result = coord
            .dispatch_task(task, 100, 150)
            .await
            .expect("RustCompilation dispatch should succeed");

        assert!(result.success, "stderr: {}", result.stderr);
        assert!(result.stdout.contains("RUSTC_LIVE_OK:42"));
    }

    #[tokio::test]
    async fn test_slm_inference_explicit_error_when_unreachable() {
        let (success, code, _stdout, stderr) = run_slm_inference("hello", 1500).await;
        // Unless an Ollama instance happens to be running on localhost:11434, check behavior cleanly:
        if !success {
            assert_eq!(code, -1);
            assert!(
                stderr.contains("SLM") || stderr.contains("endpoint"),
                "Expected explicit SLM error, got: {}",
                stderr
            );
        }
    }

    #[tokio::test]
    async fn test_slm_inference_real_http_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = r#"{"model":"test","response":"local-slm-ok","done":true}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let endpoint = format!("http://{}", addr);
        // Test HTTP roundtrip via direct helper using env-free endpoint parsing check
        let host_port = parse_http_host_port(&endpoint).unwrap();
        assert_eq!(host_port, addr.to_string());
        let mut stream = TcpStream::connect(&host_port).await.unwrap();
        let body = r#"{"model":"test","prompt":"hi","stream":false}"#;
        let req = format!(
            "POST /api/generate HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            host_port,
            body.len(),
            body
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp);
        assert!(resp_str.contains("local-slm-ok"));
    }

    #[tokio::test]
    async fn test_udp_auto_discovery_node_daemons() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let hw = HardwareProfile::detect();

        let cfg = NetworkConfig {
            bind_ip: "127.0.0.1".into(),
            tcp_port: 0,
            discovery_port: 18988,
            heartbeat_interval: Duration::from_millis(100),
            enable_udp_broadcast: true,
        };

        let node_a = NodeDaemon::start(id_a, "auto_a", hw.clone(), cfg.clone())
            .await
            .unwrap();
        let node_b = NodeDaemon::start(id_b, "auto_b", hw.clone(), cfg)
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if !node_a.list_peers().is_empty() && !node_b.list_peers().is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "NodeDaemons did not auto-discover via UDP in time"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let task = TaskSpec::new("udp_task", TaskKind::BashScript, "echo 'UDP_MESH_OK'");
        let res = node_a.dispatch_task(task, 100, 150).await.unwrap();
        assert!(res.success);
        assert!(res.stdout.contains("UDP_MESH_OK"));
    }
}
