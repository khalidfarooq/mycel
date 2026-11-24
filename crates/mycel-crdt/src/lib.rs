use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use chrono::Utc;

use mycel_proto::{CrdtDelta, PeerHeartbeat};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrdtEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub origin_peer_id: String,
    pub lamport_clock: u64,
    pub timestamp: i64,
}

#[derive(Clone, Default)]
pub struct CrdtStore {
    local_peer_id: String,
    lamport_clock: Arc<RwLock<u64>>,
    entries: Arc<RwLock<HashMap<String, CrdtEntry>>>,
    peers: Arc<RwLock<HashMap<String, PeerHeartbeat>>>,
}

impl CrdtStore {
    pub fn new(local_peer_id: impl Into<String>) -> Self {
        Self {
            local_peer_id: local_peer_id.into(),
            lamport_clock: Arc::new(RwLock::new(0)),
            entries: Arc::new(RwLock::new(HashMap::new())),
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set a key-value pair locally and return the delta to broadcast
    pub fn set(&self, key: impl Into<String>, value: Vec<u8>) -> CrdtDelta {
        let key_str = key.into();
        let mut clock_lock = self.lamport_clock.write().unwrap();
        *clock_lock += 1;
        let clock = *clock_lock;

        let now = Utc::now().timestamp_millis();
        let entry = CrdtEntry {
            key: key_str.clone(),
            value: value.clone(),
            origin_peer_id: self.local_peer_id.clone(),
            lamport_clock: clock,
            timestamp: now,
        };

        let mut entries_lock = self.entries.write().unwrap();
        entries_lock.insert(key_str.clone(), entry);

        CrdtDelta {
            key: key_str,
            value,
            origin_peer_id: self.local_peer_id.clone(),
            lamport_clock: clock,
            timestamp: now,
        }
    }

    /// Merge an incoming delta from a remote peer using Last-Write-Wins (LWW) with Lamport Clocks
    pub fn merge_delta(&self, delta: CrdtDelta) -> bool {
        // Advance local clock if remote clock is higher
        {
            let mut clock_lock = self.lamport_clock.write().unwrap();
            if delta.lamport_clock > *clock_lock {
                *clock_lock = delta.lamport_clock;
            }
        }

        let mut entries_lock = self.entries.write().unwrap();
        if let Some(existing) = entries_lock.get(&delta.key) {
            // Compare Lamport clocks
            if delta.lamport_clock > existing.lamport_clock {
                // Remote wins
                entries_lock.insert(
                    delta.key.clone(),
                    CrdtEntry {
                        key: delta.key,
                        value: delta.value,
                        origin_peer_id: delta.origin_peer_id,
                        lamport_clock: delta.lamport_clock,
                        timestamp: delta.timestamp,
                    },
                );
                return true;
            } else if delta.lamport_clock == existing.lamport_clock {
                // Tie-break with deterministic peer_id ordering
                if delta.origin_peer_id > existing.origin_peer_id {
                    entries_lock.insert(
                        delta.key.clone(),
                        CrdtEntry {
                            key: delta.key,
                            value: delta.value,
                            origin_peer_id: delta.origin_peer_id,
                            lamport_clock: delta.lamport_clock,
                            timestamp: delta.timestamp,
                        },
                    );
                    return true;
                }
            }
            false
        } else {
            // New key
            entries_lock.insert(
                delta.key.clone(),
                CrdtEntry {
                    key: delta.key,
                    value: delta.value,
                    origin_peer_id: delta.origin_peer_id,
                    lamport_clock: delta.lamport_clock,
                    timestamp: delta.timestamp,
                },
            );
            true
        }
    }

    /// Retrieve a value by key
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let entries_lock = self.entries.read().unwrap();
        entries_lock.get(key).map(|e| e.value.clone())
    }

    /// Retrieve string value
    pub fn get_string(&self, key: &str) -> Option<String> {
        self.get(key).and_then(|bytes| String::from_utf8(bytes).ok())
    }

    /// Record peer heartbeat
    pub fn record_peer_heartbeat(&self, heartbeat: PeerHeartbeat) {
        let mut peers_lock = self.peers.write().unwrap();
        peers_lock.insert(heartbeat.peer_id.clone(), heartbeat);
    }

    /// List active peers
    pub fn list_active_peers(&self) -> Vec<PeerHeartbeat> {
        let peers_lock = self.peers.read().unwrap();
        peers_lock.values().cloned().collect()
    }

    /// Current Lamport logical clock value
    pub fn lamport_clock(&self) -> u64 {
        *self.lamport_clock.read().unwrap()
    }

    /// Number of entries currently stored
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    /// Returns true if no entries are stored
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    /// Export all state as deltas for full sync upon partition recovery
    pub fn export_deltas(&self) -> Vec<CrdtDelta> {
        let entries_lock = self.entries.read().unwrap();
        entries_lock
            .values()
            .map(|e| CrdtDelta {
                key: e.key.clone(),
                value: e.value.clone(),
                origin_peer_id: e.origin_peer_id.clone(),
                lamport_clock: e.lamport_clock,
                timestamp: e.timestamp,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crdt_delta_convergence() {
        let node_a = CrdtStore::new("node_alpha");
        let node_b = CrdtStore::new("node_beta");

        // Node A sets a key
        let delta1 = node_a.set("task_101", b"running on node A".to_vec());

        // Node B receives delta1
        assert!(node_b.merge_delta(delta1));
        assert_eq!(node_b.get_string("task_101").unwrap(), "running on node A");

        // Node B updates the same key
        let delta2 = node_b.set("task_101", b"completed on node B".to_vec());

        // Node A merges delta2
        assert!(node_a.merge_delta(delta2));
        assert_eq!(node_a.get_string("task_101").unwrap(), "completed on node B");

        // Verify state identical on both nodes
        assert_eq!(node_a.get("task_101"), node_b.get("task_101"));
    }

    #[test]
    fn test_tie_breaking_deterministic() {
        let _node_a = CrdtStore::new("node_aaa");
        let _node_b = CrdtStore::new("node_zzz");

        let delta_a = CrdtDelta {
            key: "conflict".into(),
            value: b"from aaa".to_vec(),
            origin_peer_id: "node_aaa".into(),
            lamport_clock: 5,
            timestamp: 1000,
        };

        let delta_b = CrdtDelta {
            key: "conflict".into(),
            value: b"from zzz".to_vec(),
            origin_peer_id: "node_zzz".into(),
            lamport_clock: 5,
            timestamp: 1000,
        };

        let store = CrdtStore::new("local");
        store.merge_delta(delta_a);
        // delta_b has same lamport clock but origin "node_zzz" > "node_aaa"
        assert!(store.merge_delta(delta_b));
        assert_eq!(store.get_string("conflict").unwrap(), "from zzz");
    }
}
