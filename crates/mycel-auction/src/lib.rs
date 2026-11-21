use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use thiserror::Error;
use tracing::{debug, info};

use mycel_crypto::NodeIdentity;
use mycel_proto::{BidMessage, ExecutionResult, HardwareProfile, RfqMessage, TaskSpec, WorkContract};

#[derive(Error, Debug)]
pub enum AuctionError {
    #[error("No bids received within window ({0}ms)")]
    NoBidsReceived(u64),
    #[error("Invalid signature on bid or contract")]
    InvalidSignature,
    #[error("Auction not found: {0}")]
    AuctionNotFound(String),
    #[error("Hardware requirements not met for task {0}")]
    IncompatibleHardware(String),
    #[error("Task execution timeout")]
    ExecutionTimeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuctionState {
    Open,
    BiddingClosed,
    Awarded,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct AuctionSession {
    pub rfq: RfqMessage,
    pub state: AuctionState,
    pub bids: Vec<BidMessage>,
    pub awarded_contract: Option<WorkContract>,
    pub result: Option<ExecutionResult>,
}

#[derive(Clone)]
pub struct AuctionEngine {
    identity: NodeIdentity,
    hardware: HardwareProfile,
    sessions: Arc<Mutex<HashMap<String, AuctionSession>>>,
}

impl AuctionEngine {
    pub fn new(identity: NodeIdentity, hardware: HardwareProfile) -> Self {
        Self {
            identity,
            hardware,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create and broadcast a new Request For Quote (RFQ)
    pub async fn create_rfq(&self, task: TaskSpec, max_budget: u64, window_ms: u64) -> RfqMessage {
        let mut rfq = RfqMessage::new(self.identity.peer_id(), task, max_budget);
        rfq.bid_window_ms = window_ms;

        let session = AuctionSession {
            rfq: rfq.clone(),
            state: AuctionState::Open,
            bids: Vec::new(),
            awarded_contract: None,
            result: None,
        };

        let mut lock = self.sessions.lock().await;
        lock.insert(rfq.rfq_id.clone(), session);
        info!(rfq_id = %rfq.rfq_id, task_id = %rfq.task.task_id, "Initialized new Spore compute auction");
        rfq
    }

    /// Evaluate an incoming RFQ from another node to determine whether to place a bid
    pub fn evaluate_rfq(&self, rfq: &RfqMessage) -> Result<Option<BidMessage>, AuctionError> {
        if rfq.originator_peer_id == self.identity.peer_id() {
            return Ok(None); // Do not bid on own auction
        }

        // Check hardware compatibility
        if rfq.task.min_cpu_cores > self.hardware.cpu_cores {
            return Ok(None);
        }
        if rfq.task.min_ram_mb > self.hardware.ram_mb {
            return Ok(None);
        }
        if rfq.task.requires_metal && !self.hardware.has_metal {
            return Ok(None);
        }
        if rfq.task.requires_cuda && !self.hardware.has_cuda {
            return Ok(None);
        }

        // Calculate competitive bid
        // Lower load = lower price + higher score
        let base_cost = 10u64;
        let load_multiplier = 1.0 + self.hardware.load_average;
        let calculated_price = (base_cost as f32 * load_multiplier) as u64;
        let price = calculated_price.min(rfq.max_budget);

        let estimated_duration_ms = 100 + (self.hardware.load_average * 50.0) as u64;
        let node_score = (self.hardware.cpu_cores as f32 * 10.0) / (load_multiplier + 0.1);

        let mut bid = BidMessage::new(
            &rfq.rfq_id,
            self.identity.peer_id(),
            price,
            estimated_duration_ms,
            node_score,
        );

        // Sign the bid with our node private key
        let payload = format!("{}:{}:{}", bid.rfq_id, bid.bid_id, bid.price);
        bid.signature = self.identity.sign(payload.as_bytes());

        debug!(rfq_id = %rfq.rfq_id, price = %bid.price, "Generated competitive bid for task");
        Ok(Some(bid))
    }

    /// Register an incoming bid for an active auction
    pub async fn record_bid(&self, bid: BidMessage) -> Result<(), AuctionError> {
        let mut lock = self.sessions.lock().await;
        if let Some(session) = lock.get_mut(&bid.rfq_id) {
            if session.state == AuctionState::Open {
                session.bids.push(bid);
                return Ok(());
            }
        }
        Ok(())
    }

    /// Resolve an auction after the bidding window closes and award contract to best bidder
    pub async fn resolve_auction(&self, rfq_id: &str) -> Result<WorkContract, AuctionError> {
        let mut lock = self.sessions.lock().await;
        let session = lock
            .get_mut(rfq_id)
            .ok_or_else(|| AuctionError::AuctionNotFound(rfq_id.to_string()))?;

        session.state = AuctionState::BiddingClosed;

        if session.bids.is_empty() {
            session.state = AuctionState::Failed;
            return Err(AuctionError::NoBidsReceived(session.rfq.bid_window_ms));
        }

        // Rank bids by score / price ratio
        let mut ranked = session.bids.clone();
        ranked.sort_by(|a, b| {
            let score_a = a.node_score / ((a.price + 1) as f32 * (a.estimated_duration_ms + 1) as f32);
            let score_b = b.node_score / ((b.price + 1) as f32 * (b.estimated_duration_ms + 1) as f32);
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        let winner = ranked[0].clone();
        let mut contract = WorkContract::new(
            rfq_id,
            self.identity.peer_id(),
            &winner.bidder_peer_id,
            session.rfq.task.clone(),
            winner.price,
        );

        let contract_payload = format!("{}:{}:{}", contract.contract_id, contract.assigned_worker_peer_id, contract.agreed_price);
        contract.signature = self.identity.sign(contract_payload.as_bytes());

        session.state = AuctionState::Awarded;
        session.awarded_contract = Some(contract.clone());

        info!(
            rfq_id = %rfq_id,
            contract_id = %contract.contract_id,
            winner = %contract.assigned_worker_peer_id,
            price = %contract.agreed_price,
            "Auction awarded successfully"
        );

        Ok(contract)
    }

    /// Record completed result
    pub async fn complete_auction(&self, result: ExecutionResult) -> Result<(), AuctionError> {
        let mut lock = self.sessions.lock().await;
        for session in lock.values_mut() {
            if let Some(ref contract) = session.awarded_contract {
                if contract.contract_id == result.contract_id {
                    session.state = AuctionState::Completed;
                    session.result = Some(result);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub async fn get_session(&self, rfq_id: &str) -> Option<AuctionSession> {
        let lock = self.sessions.lock().await;
        lock.get(rfq_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycel_proto::TaskKind;

    #[tokio::test]
    async fn test_full_auction_lifecycle() {
        let originator_id = NodeIdentity::generate();
        let worker_id = NodeIdentity::generate();

        let hw = HardwareProfile {
            cpu_cores: 16,
            ram_mb: 65536,
            has_metal: true,
            has_cuda: false,
            available_slms: vec!["gemma-2-2b".to_string()],
            load_average: 0.15,
        };

        let auctioneer = AuctionEngine::new(originator_id.clone(), hw.clone());
        let worker = AuctionEngine::new(worker_id.clone(), hw.clone());

        let task = TaskSpec::new("test_suite", TaskKind::TestSuite, "cargo test");
        let rfq = auctioneer.create_rfq(task, 100, 200).await;

        // Worker receives RFQ and bids
        let bid_opt = worker.evaluate_rfq(&rfq).expect("Evaluation should succeed");
        assert!(bid_opt.is_some());
        let bid = bid_opt.unwrap();
        assert_eq!(bid.bidder_peer_id, worker_id.peer_id());

        // Auctioneer records bid
        auctioneer.record_bid(bid).await.expect("Record bid should succeed");

        // Resolve auction
        let contract = auctioneer.resolve_auction(&rfq.rfq_id).await.expect("Auction resolution should succeed");
        assert_eq!(contract.assigned_worker_peer_id, worker_id.peer_id());

        // Worker submits execution result
        let result = ExecutionResult {
            contract_id: contract.contract_id.clone(),
            worker_peer_id: worker_id.peer_id().to_string(),
            success: true,
            exit_code: 0,
            stdout: "10 passed, 0 failed".into(),
            stderr: String::new(),
            execution_time_ms: 120,
            signature: worker_id.sign(b"receipt"),
        };

        auctioneer.complete_auction(result).await.expect("Complete auction should succeed");
        let session = auctioneer.get_session(&rfq.rfq_id).await.unwrap();
        assert_eq!(session.state, AuctionState::Completed);
    }
}
