use std::net::SocketAddr;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HardwareProfile {
    pub cpu_cores: usize,
    pub ram_mb: u64,
    pub has_metal: bool,
    pub has_cuda: bool,
    pub available_slms: Vec<String>,
    pub load_average: f32,
}

impl HardwareProfile {
    /// Detect live host hardware metrics from the operating system.
    pub fn detect() -> Self {
        Self {
            cpu_cores: detect_cpu_cores(),
            ram_mb: detect_ram_mb(),
            has_metal: cfg!(target_os = "macos"),
            has_cuda: detect_cuda(),
            available_slms: detect_configured_slms(),
            load_average: detect_load_average(),
        }
    }
}

impl Default for HardwareProfile {
    fn default() -> Self {
        Self::detect()
    }
}

fn detect_cpu_cores() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

#[cfg(target_os = "macos")]
fn detect_ram_mb() -> u64 {
    let name = b"hw.memsize\0";
    let mut memsize: u64 = 0;
    let mut size = std::mem::size_of::<u64>();
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr() as *const libc::c_char,
            &mut memsize as *mut u64 as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 {
        memsize / (1024 * 1024)
    } else {
        0
    }
}

#[cfg(target_os = "linux")]
fn detect_ram_mb() -> u64 {
    if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
        for line in contents.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                if let Some(kb_str) = rest.split_whitespace().next() {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        return kb / 1024;
                    }
                }
            }
        }
    }
    0
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_ram_mb() -> u64 {
    0
}

#[cfg(unix)]
fn detect_load_average() -> f32 {
    let mut avg = [0.0f64; 3];
    let count = unsafe { libc::getloadavg(avg.as_mut_ptr(), 1) };
    if count >= 1 && avg[0].is_finite() && avg[0] >= 0.0 {
        avg[0] as f32
    } else {
        0.0
    }
}

#[cfg(not(unix))]
fn detect_load_average() -> f32 {
    0.0
}

fn detect_cuda() -> bool {
    #[cfg(target_os = "macos")]
    {
        false
    }
    #[cfg(not(target_os = "macos"))]
    {
        if std::path::Path::new("/dev/nvidia0").exists() {
            return true;
        }
        std::process::Command::new("nvidia-smi")
            .arg("-L")
            .output()
            .map(|out| out.status.success())
            .unwrap_or(false)
    }
}

fn detect_configured_slms() -> Vec<String> {
    match std::env::var("MYCEL_SLM_MODEL") {
        Ok(val) => val
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskKind {
    BashScript,
    RustCompilation,
    TestSuite,
    SlmInference,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub task_id: String,
    pub name: String,
    pub kind: TaskKind,
    pub payload: String,
    pub working_dir: Option<String>,
    pub timeout_ms: u64,
    pub min_cpu_cores: usize,
    pub min_ram_mb: u64,
    pub requires_metal: bool,
    pub requires_cuda: bool,
}

impl TaskSpec {
    pub fn new(name: impl Into<String>, kind: TaskKind, payload: impl Into<String>) -> Self {
        Self {
            task_id: format!("task_{}", Uuid::new_v4().simple()),
            name: name.into(),
            kind,
            payload: payload.into(),
            working_dir: None,
            timeout_ms: 30_000,
            min_cpu_cores: 1,
            min_ram_mb: 256,
            requires_metal: false,
            requires_cuda: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RfqMessage {
    pub rfq_id: String,
    pub originator_peer_id: String,
    pub task: TaskSpec,
    pub max_budget: u64,
    pub bid_window_ms: u64,
    pub created_at: i64,
}

impl RfqMessage {
    pub fn new(originator: impl Into<String>, task: TaskSpec, max_budget: u64) -> Self {
        Self {
            rfq_id: format!("rfq_{}", Uuid::new_v4().simple()),
            originator_peer_id: originator.into(),
            task,
            max_budget,
            bid_window_ms: 250,
            created_at: Utc::now().timestamp_millis(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BidMessage {
    pub rfq_id: String,
    pub bid_id: String,
    pub bidder_peer_id: String,
    pub bidder_public_key_hex: String,
    pub price: u64,
    pub estimated_duration_ms: u64,
    pub node_score: f32,
    pub signature: String,
}

impl BidMessage {
    pub fn new(
        rfq_id: impl Into<String>,
        bidder: impl Into<String>,
        bidder_public_key_hex: impl Into<String>,
        price: u64,
        estimated_duration_ms: u64,
        node_score: f32,
    ) -> Self {
        Self {
            rfq_id: rfq_id.into(),
            bid_id: format!("bid_{}", Uuid::new_v4().simple()),
            bidder_peer_id: bidder.into(),
            bidder_public_key_hex: bidder_public_key_hex.into(),
            price,
            estimated_duration_ms,
            node_score,
            signature: String::new(),
        }
    }

    pub fn signing_payload(&self) -> Vec<u8> {
        format!(
            "bid:{}:{}:{}:{}:{}",
            self.rfq_id, self.bid_id, self.bidder_peer_id, self.price, self.estimated_duration_ms
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkContract {
    pub contract_id: String,
    pub rfq_id: String,
    pub originator_peer_id: String,
    pub originator_public_key_hex: String,
    pub assigned_worker_peer_id: String,
    pub task: TaskSpec,
    pub agreed_price: u64,
    pub awarded_at: i64,
    pub signature: String,
}

impl WorkContract {
    pub fn new(
        rfq_id: impl Into<String>,
        originator: impl Into<String>,
        originator_public_key_hex: impl Into<String>,
        worker: impl Into<String>,
        task: TaskSpec,
        agreed_price: u64,
    ) -> Self {
        Self {
            contract_id: format!("contract_{}", Uuid::new_v4().simple()),
            rfq_id: rfq_id.into(),
            originator_peer_id: originator.into(),
            originator_public_key_hex: originator_public_key_hex.into(),
            assigned_worker_peer_id: worker.into(),
            task,
            agreed_price,
            awarded_at: Utc::now().timestamp_millis(),
            signature: String::new(),
        }
    }

    pub fn signing_payload(&self) -> Vec<u8> {
        format!(
            "contract:{}:{}:{}:{}:{}:{}",
            self.contract_id,
            self.rfq_id,
            self.originator_peer_id,
            self.assigned_worker_peer_id,
            self.agreed_price,
            self.task.task_id
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub contract_id: String,
    pub worker_peer_id: String,
    pub worker_public_key_hex: String,
    pub success: bool,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub execution_time_ms: u64,
    pub signature: String,
}

impl ExecutionResult {
    pub fn signing_payload(&self) -> Vec<u8> {
        format!(
            "result:{}:{}:{}:{}:{}:{}:{}",
            self.contract_id,
            self.worker_peer_id,
            self.success,
            self.exit_code,
            self.execution_time_ms,
            self.stdout.len(),
            self.stderr.len()
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerHeartbeat {
    pub peer_id: String,
    pub node_name: String,
    pub hardware: HardwareProfile,
    pub timestamp: i64,
    pub uptime_secs: u64,
    pub active_tasks: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PeerDiscoveryBeacon {
    pub peer_id: String,
    pub public_key_hex: String,
    pub node_name: String,
    pub actual_tcp_addr: SocketAddr,
    pub hardware: HardwareProfile,
    pub timestamp: i64,
    pub signature: String,
}

impl PeerDiscoveryBeacon {
    pub fn signing_payload(&self) -> Vec<u8> {
        format!(
            "beacon:{}:{}:{}:{}",
            self.peer_id, self.node_name, self.actual_tcp_addr, self.timestamp
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedHandshake {
    pub peer_id: String,
    pub public_key_hex: String,
    pub ephemeral_pubkey: [u8; 32],
    pub tcp_listen_port: u16,
    pub heartbeat: PeerHeartbeat,
    pub signature: String,
}

impl SignedHandshake {
    pub fn signing_payload(
        peer_id: &str,
        public_key_hex: &str,
        ephemeral_pubkey: &[u8; 32],
        tcp_listen_port: u16,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(b"mycel-handshake-v1:");
        buf.extend_from_slice(peer_id.as_bytes());
        buf.push(b':');
        buf.extend_from_slice(public_key_hex.as_bytes());
        buf.push(b':');
        buf.extend_from_slice(ephemeral_pubkey);
        buf.extend_from_slice(&tcp_listen_port.to_be_bytes());
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrdtDelta {
    pub key: String,
    pub value: Vec<u8>,
    pub origin_peer_id: String,
    pub lamport_clock: u64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MeshMessage {
    Heartbeat(PeerHeartbeat),
    Rfq(RfqMessage),
    Bid(BidMessage),
    ContractAward(WorkContract),
    Result(ExecutionResult),
    Delta(CrdtDelta),
    CrdtSnapshot(Vec<CrdtDelta>),
}

impl MeshMessage {
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_serialization() {
        let task = TaskSpec::new("cargo_test", TaskKind::TestSuite, "cargo test --workspace");
        let rfq = RfqMessage::new("node_macbook", task, 100);
        let msg = MeshMessage::Rfq(rfq.clone());

        let bytes = msg.to_bytes().expect("serialization should succeed");
        let deserialized = MeshMessage::from_bytes(&bytes).expect("deserialization should succeed");

        match deserialized {
            MeshMessage::Rfq(deserialized_rfq) => {
                assert_eq!(rfq.rfq_id, deserialized_rfq.rfq_id);
                assert_eq!(rfq.task.name, "cargo_test");
            }
            _ => panic!("Expected Rfq variant"),
        }
    }

    #[test]
    fn test_hardware_profile_detect() {
        let hw = HardwareProfile::detect();
        assert!(hw.cpu_cores >= 1);
        assert!(hw.ram_mb > 0, "Expected real RAM > 0 MB on macOS/Linux");
        assert!(hw.load_average >= 0.0);
    }
}
