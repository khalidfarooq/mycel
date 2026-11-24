use std::time::Duration;

use mycel_auction::AuctionEngine;
use mycel_crypto::NodeIdentity;
use mycel_net::NetworkConfig;
use mycel_node::NodeDaemon;
use mycel_proto::{HardwareProfile, TaskKind, TaskSpec};

#[tokio::test]
async fn test_e2e_three_node_auction_and_crdt() {
    let id_a = NodeIdentity::generate();
    let id_b = NodeIdentity::generate();
    let id_c = NodeIdentity::generate();

    let hw_a = HardwareProfile {
        cpu_cores: 8,
        ram_mb: 16384,
        has_metal: cfg!(target_os = "macos"),
        has_cuda: false,
        available_slms: vec![],
        load_average: 0.1,
    };
    let hw_b = HardwareProfile {
        cpu_cores: 32,
        ram_mb: 65536,
        has_metal: false,
        has_cuda: true,
        available_slms: vec![],
        load_average: 0.2,
    };
    let hw_c = HardwareProfile {
        cpu_cores: 4,
        ram_mb: 8192,
        has_metal: false,
        has_cuda: false,
        available_slms: vec![],
        load_average: 0.05,
    };

    let node_a = NodeDaemon::start(id_a.clone(), "node_alpha", hw_a, NetworkConfig::default())
        .await
        .unwrap();
    let node_b = NodeDaemon::start(id_b.clone(), "node_beta", hw_b, NetworkConfig::default())
        .await
        .unwrap();
    let node_c = NodeDaemon::start(id_c.clone(), "node_gamma", hw_c, NetworkConfig::default())
        .await
        .unwrap();

    // Connect B and C to A over X25519 + ChaCha20-Poly1305 encrypted TCP
    node_b
        .network
        .connect_peer(node_a.network.actual_tcp_addr)
        .await
        .unwrap();
    node_c
        .network
        .connect_peer(node_a.network.actual_tcp_addr)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Node A dispatches task
    let task = TaskSpec::new(
        "e2e_test_task",
        TaskKind::BashScript,
        "echo 'MYCEL_E2E_VERIFIED'",
    );
    let (result, session) = node_a
        .dispatch_task_with_session(task, 100, 150)
        .await
        .expect("E2E task dispatch should succeed");

    assert!(result.success);
    assert!(result.stdout.contains("MYCEL_E2E_VERIFIED"));
    assert_eq!(session.bids.len(), 2);

    // Verify cryptographic Ed25519 signature on receipt
    AuctionEngine::verify_result_signature(&result)
        .expect("ExecutionResult signature must verify against worker public key");

    // Replicate custom CRDT state from A across encrypted links to B and C
    node_a
        .set_state("mesh/build_tag", b"v0.1.0-verified".to_vec())
        .await
        .unwrap();

    let contract_key = format!("contract:{}", result.contract_id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let b_has = node_b.crdt.get_string("mesh/build_tag").as_deref() == Some("v0.1.0-verified");
        let c_has = node_c.crdt.get_string("mesh/build_tag").as_deref() == Some("v0.1.0-verified");
        let b_contract = node_b.crdt.get(&contract_key).is_some();
        let c_contract = node_c.crdt.get(&contract_key).is_some();
        if b_has && c_has && b_contract && c_contract {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "CRDT deltas did not converge across peers"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(node_a.crdt.lamport_clock(), node_b.crdt.lamport_clock());
    assert_eq!(node_a.crdt.lamport_clock(), node_c.crdt.lamport_clock());

    // Late-joining peer receives full CRDT snapshot on connect
    let id_d = NodeIdentity::generate();
    let node_d = NodeDaemon::start(
        id_d,
        "node_delta_late",
        HardwareProfile::detect(),
        NetworkConfig::default(),
    )
    .await
    .unwrap();
    node_d
        .network
        .connect_peer(node_a.network.actual_tcp_addr)
        .await
        .unwrap();

    let snap_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if node_d.crdt.get_string("mesh/build_tag").as_deref() == Some("v0.1.0-verified")
            && node_d.crdt.get(&contract_key).is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < snap_deadline,
            "Late-joining peer did not receive CRDT snapshot"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(node_d.crdt.lamport_clock(), node_a.crdt.lamport_clock());
}
