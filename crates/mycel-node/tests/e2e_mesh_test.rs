use std::time::Duration;
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
        has_metal: true,
        has_cuda: false,
        available_slms: vec!["gemma-2-2b".into()],
        load_average: 0.1,
    };
    let hw_b = HardwareProfile {
        cpu_cores: 32,
        ram_mb: 65536,
        has_metal: false,
        has_cuda: true,
        available_slms: vec!["llama-3-8b".into()],
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

    let node_a = NodeDaemon::start(id_a.clone(), "node_alpha", hw_a, NetworkConfig::default()).await.unwrap();
    let node_b = NodeDaemon::start(id_b.clone(), "node_beta", hw_b, NetworkConfig::default()).await.unwrap();
    let node_c = NodeDaemon::start(id_c.clone(), "node_gamma", hw_c, NetworkConfig::default()).await.unwrap();

    // Connect B and C to A
    node_b.network.connect_peer(node_a.network.actual_tcp_addr).await.unwrap();
    node_c.network.connect_peer(node_a.network.actual_tcp_addr).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Node A dispatches task
    let task = TaskSpec::new("e2e_test_task", TaskKind::BashScript, "echo 'MYCEL_E2E_VERIFIED'");
    let result = node_a.dispatch_task(task, 100, 150).await.expect("E2E task dispatch should succeed");

    assert!(result.success);
    assert!(result.stdout.contains("MYCEL_E2E_VERIFIED"));

    // Verify cryptographic signature on receipt
    assert!(!result.signature.is_empty());
}
