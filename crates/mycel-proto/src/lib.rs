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

impl Default for HardwareProfile {
    fn default() -> Self {
        Self {
            cpu_cores: num_cpus(),
            ram_mb: 16384,
            has_metal: cfg!(target_os = "macos"),
            has_cuda: false,
            available_slms: vec!["gemma-2-2b".to_string()],
            load_average: 0.1,
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskKind {
    BashScript,
    RustCompilation,
    TestSuite,
    SlmInference,
    WasmExecution,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub task_id: String,
    pub name: String,
    pub kind: TaskKind,
    pub payload: String,
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
            timeout_ms: 30_000,
            min_cpu_cores: 1,
            min_ram_mb: 512,
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
    pub price: u64,
    pub estimated_duration_ms: u64,
    pub node_score: f32,
    pub signature: String,
}

impl BidMessage {
    pub fn new(
        rfq_id: impl Into<String>,
        bidder: impl Into<String>,
        price: u64,
        estimated_duration_ms: u64,
        node_score: f32,
    ) -> Self {
        Self {
            rfq_id: rfq_id.into(),
            bid_id: format!("bid_{}", Uuid::new_v4().simple()),
            bidder_peer_id: bidder.into(),
            price,
            estimated_duration_ms,
            node_score,
            signature: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkContract {
    pub contract_id: String,
    pub rfq_id: String,
    pub originator_peer_id: String,
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
        worker: impl Into<String>,
        task: TaskSpec,
        agreed_price: u64,
    ) -> Self {
        Self {
            contract_id: format!("contract_{}", Uuid::new_v4().simple()),
            rfq_id: rfq_id.into(),
            originator_peer_id: originator.into(),
            assigned_worker_peer_id: worker.into(),
            task,
            agreed_price,
            awarded_at: Utc::now().timestamp_millis(),
            signature: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub contract_id: String,
    pub worker_peer_id: String,
    pub success: bool,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub execution_time_ms: u64,
    pub signature: String,
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
}
