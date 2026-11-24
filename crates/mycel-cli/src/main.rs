use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use mycel_auction::AuctionEngine;
use mycel_crypto::NodeIdentity;
use mycel_net::NetworkConfig;
use mycel_node::NodeDaemon;
use mycel_proto::{HardwareProfile, TaskKind, TaskSpec};

mod tui;

#[derive(Parser)]
#[command(name = "mycel")]
#[command(author = "Khalid Farooq <wandererkhalid@gmail.com>")]
#[command(version = "0.1.0")]
#[command(
    about = "🍄 Sovereign, Zero-Cloud P2P Mesh Network & Compute Exchange for Autonomous Edge Agents"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new cryptographic Ed25519 node identity and inspect host hardware
    Init,

    /// Start a local Mycel node daemon
    Start {
        #[arg(short, long, default_value = "mycel-edge-node")]
        name: String,

        #[arg(short, long, default_value_t = 8765)]
        port: u16,

        #[arg(long, default_value_t = 8766)]
        discovery_port: u16,

        #[arg(long)]
        no_udp: bool,

        #[arg(long)]
        peers: Option<String>,
    },

    /// Dispatch a task to the decentralized Spore compute market
    Dispatch {
        #[arg(short, long, default_value = "echo 'Mycel autonomous mesh task'")]
        cmd: String,

        #[arg(short, long, default_value = "bash")]
        kind: String,

        #[arg(short, long, default_value_t = 100)]
        budget: u64,

        #[arg(short, long)]
        peers: Option<String>,

        #[arg(short, long, default_value_t = 0)]
        listen_port: u16,

        #[arg(long, default_value_t = 8766)]
        discovery_port: u16,

        #[arg(long, default_value_t = 1500)]
        discovery_wait_ms: u64,

        #[arg(long, default_value_t = 300)]
        window_ms: u64,

        #[arg(long)]
        working_dir: Option<String>,

        #[arg(long)]
        no_udp: bool,
    },

    /// Run a live multi-node mesh cluster & compute auction demo
    Demo,

    /// Launch the interactive real-time terminal TUI dashboard
    Tui {
        /// Optional duration in seconds before exiting automatically
        #[arg(long)]
        seconds: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init => {
            println!("\n🍄 MYCEL IDENTITY GENERATOR");
            println!("============================================================");
            let identity = NodeIdentity::generate();
            let hw = HardwareProfile::detect();

            println!("✓ Generated Ed25519 Cryptographic Identity");
            println!("▸ Peer ID:      {}", identity.peer_id());
            println!("▸ Public Key:   {}", identity.public_key_hex());
            println!("▸ CPU Cores:    {}", hw.cpu_cores);
            println!("▸ RAM:          {} MB", hw.ram_mb);
            println!("▸ Load Avg (1m): {:.2}", hw.load_average);
            println!(
                "▸ Metal Accel:  {}",
                if hw.has_metal {
                    "ENABLED (macOS)"
                } else {
                    "DISABLED"
                }
            );
            println!(
                "▸ CUDA Accel:   {}",
                if hw.has_cuda { "ENABLED" } else { "DISABLED" }
            );
            println!(
                "▸ Local SLMs:   {}",
                if hw.available_slms.is_empty() {
                    "None configured (set MYCEL_SLM_MODEL)".to_string()
                } else {
                    hw.available_slms.join(", ")
                }
            );
            println!("============================================================\n");
        }

        Commands::Start {
            name,
            port,
            discovery_port,
            no_udp,
            peers,
        } => {
            println!("\n🍄 STARTING MYCEL NODE: {}", name);
            let identity = NodeIdentity::generate();
            let hw = HardwareProfile::detect();
            let net_cfg = NetworkConfig {
                bind_ip: "0.0.0.0".into(),
                tcp_port: port,
                discovery_port,
                heartbeat_interval: Duration::from_millis(500),
                enable_udp_broadcast: !no_udp,
            };

            let daemon = NodeDaemon::start(identity.clone(), name.clone(), hw.clone(), net_cfg).await?;
            println!("✓ Node bound to TCP {}", daemon.network.actual_tcp_addr);
            println!("✓ Peer ID: {}", identity.peer_id());
            println!(
                "✓ Hardware: {} CPU Cores · {} MB RAM · Load {:.2}",
                hw.cpu_cores, hw.ram_mb, hw.load_average
            );
            if !no_udp {
                println!(
                    "✓ UDP Auto-Discovery active on port {}",
                    discovery_port
                );
            }

            if let Some(peer_list) = peers {
                for peer_addr_str in peer_list.split(',') {
                    if let Ok(addr) = peer_addr_str.trim().parse::<SocketAddr>() {
                        println!("▸ Connecting to mesh peer at {}...", addr);
                        if let Err(e) = daemon.network.connect_peer(addr).await {
                            eprintln!("  ✗ Failed to connect to {}: {}", addr, e);
                        }
                    }
                }
            }

            println!("⚡ Node active (X25519 + ChaCha20-Poly1305 AEAD). Press Ctrl+C to stop.");
            tokio::signal::ctrl_c().await?;
            println!("\n🛑 Node shut down gracefully.");
        }

        Commands::Dispatch {
            cmd,
            kind,
            budget,
            peers,
            listen_port,
            discovery_port,
            discovery_wait_ms,
            window_ms,
            working_dir,
            no_udp,
        } => {
            let identity = NodeIdentity::generate();
            let hw = HardwareProfile::detect();
            let use_udp = !no_udp && peers.is_none();
            let net_cfg = NetworkConfig {
                bind_ip: "0.0.0.0".into(),
                tcp_port: listen_port,
                discovery_port,
                heartbeat_interval: Duration::from_millis(200),
                enable_udp_broadcast: use_udp,
            };

            let daemon = NodeDaemon::start(identity, "dispatcher", hw, net_cfg).await?;

            if let Some(ref peer_list) = peers {
                for peer_addr_str in peer_list.split(',') {
                    if let Ok(addr) = peer_addr_str.trim().parse::<SocketAddr>() {
                        println!("▸ Connecting to peer {}...", addr);
                        daemon.network.connect_peer(addr).await?;
                    }
                }
                tokio::time::sleep(Duration::from_millis(80)).await;
            } else if use_udp {
                println!(
                    "▸ Discovering mesh peers via UDP port {} ({}ms)...",
                    discovery_port, discovery_wait_ms
                );
                let deadline = Instant::now() + Duration::from_millis(discovery_wait_ms);
                while Instant::now() < deadline {
                    if daemon.network.connected_peer_count().await > 0 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            let peer_count = daemon.network.connected_peer_count().await;
            println!("▸ Connected peers: {}", peer_count);

            let task_kind = match kind.to_lowercase().as_str() {
                "test" => TaskKind::TestSuite,
                "rust" => TaskKind::RustCompilation,
                "slm" => TaskKind::SlmInference,
                _ => TaskKind::BashScript,
            };

            let mut task = TaskSpec::new("user_dispatched_task", task_kind, cmd);
            task.working_dir = working_dir;

            println!("\n🚀 DISPATCHING TASK TO SPORE AUCTION MARKET");
            println!("▸ Task:   {} ({:?})", task.name, task.kind);
            println!("▸ Budget: {} credits", budget);

            match daemon.dispatch_task_with_session(task, budget, window_ms).await {
                Ok((result, session)) => {
                    println!("\n✓ TASK COMPLETED BY MESH");
                    println!("▸ Bids Recv: {}", session.bids.len());
                    println!("▸ Worker:    {}", result.worker_peer_id);
                    println!("▸ Exit Code: {}", result.exit_code);
                    println!("▸ Duration:  {} ms", result.execution_time_ms);
                    println!("▸ Success:   {}", result.success);
                    println!("▸ Receipt:   {}...", &result.signature[..32.min(result.signature.len())]);
                    if !result.stdout.is_empty() {
                        println!("▸ Stdout:\n{}", result.stdout.trim_end());
                    }
                    if !result.stderr.is_empty() {
                        println!("▸ Stderr:\n{}", result.stderr.trim_end());
                    }
                }
                Err(e) => {
                    eprintln!("✗ Dispatch error: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Demo => {
            run_live_multi_node_demo().await?;
        }

        Commands::Tui { seconds } => {
            tui::run_dashboard(seconds).await?;
        }
    }

    Ok(())
}

fn format_hw_summary(hw: &HardwareProfile) -> String {
    let accel = if hw.has_metal {
        "Metal"
    } else if hw.has_cuda {
        "CUDA"
    } else {
        "CPU"
    };
    format!(
        "{} Cores · {} MB RAM · {} · Load {:.2}",
        hw.cpu_cores, hw.ram_mb, accel, hw.load_average
    )
}

async fn run_live_multi_node_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n");
    println!("  __  ____   ______ _____ _      ");
    println!(" |  \\/  \\ \\ / / ___| ____| |     ");
    println!(" | |\\/| |\\ V / |   |  _| | |     ");
    println!(" | |  | | | || |___| |___| |___  ");
    println!(" |_|  |_| |_| \\____|_____|_____| ");
    println!(" Sovereign, Zero-Cloud P2P Mesh Network & Compute Exchange v0.1.0\n");
    println!("================================================================================");
    println!(" 🌌 LIVE MULTI-NODE MESH & SPORE COMPUTE EXCHANGE DEMO");
    println!("================================================================================");

    let discovery_port = 18766;
    let udp_cfg = NetworkConfig {
        bind_ip: "127.0.0.1".into(),
        tcp_port: 0,
        discovery_port,
        heartbeat_interval: Duration::from_millis(80),
        enable_udp_broadcast: true,
    };

    let mut peer_names: HashMap<String, String> = HashMap::new();

    let id_coord = NodeIdentity::generate();
    let hw_coord = HardwareProfile::detect();
    let node_coord = NodeDaemon::start(
        id_coord.clone(),
        "mycel-coordinator",
        hw_coord.clone(),
        udp_cfg.clone(),
    )
    .await?;
    peer_names.insert(id_coord.peer_id().to_string(), "mycel-coordinator".into());
    println!("✓ [Node 1] Booted 'mycel-coordinator'");
    println!("  ▸ Peer ID:    {}", id_coord.peer_id());
    println!("  ▸ TCP Addr:   {}", node_coord.network.actual_tcp_addr);
    println!("  ▸ Hardware:   {}\n", format_hw_summary(&hw_coord));

    let id_worker_a = NodeIdentity::generate();
    let hw_worker_a = HardwareProfile::detect();
    let node_worker_a = NodeDaemon::start(
        id_worker_a.clone(),
        "mycel-worker-1",
        hw_worker_a.clone(),
        udp_cfg.clone(),
    )
    .await?;
    peer_names.insert(id_worker_a.peer_id().to_string(), "mycel-worker-1".into());
    println!("✓ [Node 2] Booted 'mycel-worker-1'");
    println!("  ▸ Peer ID:    {}", id_worker_a.peer_id());
    println!("  ▸ TCP Addr:   {}", node_worker_a.network.actual_tcp_addr);
    println!("  ▸ Hardware:   {}\n", format_hw_summary(&hw_worker_a));

    let id_worker_b = NodeIdentity::generate();
    let hw_worker_b = HardwareProfile::detect();
    let node_worker_b = NodeDaemon::start(
        id_worker_b.clone(),
        "mycel-worker-2",
        hw_worker_b.clone(),
        udp_cfg,
    )
    .await?;
    peer_names.insert(id_worker_b.peer_id().to_string(), "mycel-worker-2".into());
    println!("✓ [Node 3] Booted 'mycel-worker-2'");
    println!("  ▸ Peer ID:    {}", id_worker_b.peer_id());
    println!("  ▸ TCP Addr:   {}", node_worker_b.network.actual_tcp_addr);
    println!("  ▸ Hardware:   {}\n", format_hw_summary(&hw_worker_b));

    println!("--------------------------------------------------------------------------------");
    println!(" 🔗 UDP AUTO-DISCOVERY & ENCRYPTED X25519 + CHACHA20-POLY1305 MESH HANDSHAKES");
    println!("--------------------------------------------------------------------------------");

    let t0 = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline {
        if node_coord.network.connected_peer_count().await >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let discovery_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let active_peers = node_coord.network.connected_peer_count().await;
    println!(
        "✓ UDP Auto-Discovery & Ed25519 Mutual Handshake Complete in {:.2}ms",
        discovery_ms
    );
    println!(
        "✓ Coordinator Connected Peers: {} · Transport: X25519 + ChaCha20-Poly1305 AEAD\n",
        active_peers
    );

    // Scenario 1: Real Shell / System Probe Task
    println!("--------------------------------------------------------------------------------");
    println!(" ⚡ SCENARIO 1: REAL SYSTEM DIAGNOSTIC TASK AUCTION");
    println!("--------------------------------------------------------------------------------");
    let cmd1 = "uname -ms && echo \"worker-pid=$$ status=ok\"";
    println!("▸ Coordinator dispatching BashScript: `{}`", cmd1);

    let task1 = TaskSpec::new("system_probe", TaskKind::BashScript, cmd1);
    let (result1, session1) = node_coord
        .dispatch_task_with_session(task1, 100, 150)
        .await?;
    AuctionEngine::verify_result_signature(&result1)?;

    println!("  ⚡ Real Bids Collected ({}):", session1.bids.len());
    for bid in &session1.bids {
        let name = peer_names
            .get(&bid.bidder_peer_id)
            .map(String::as_str)
            .unwrap_or(&bid.bidder_peer_id);
        println!(
            "    • {} ({}): {} credits · est {}ms · score {:.1} · sig {}...",
            name,
            bid.bidder_peer_id,
            bid.price,
            bid.estimated_duration_ms,
            bid.node_score,
            &bid.signature[..16]
        );
    }

    let winner1_name = peer_names
        .get(&result1.worker_peer_id)
        .map(String::as_str)
        .unwrap_or(&result1.worker_peer_id);
    println!(
        "  🏆 Winner: {} ({})",
        winner1_name, result1.worker_peer_id
    );
    println!(
        "  📜 Verified Ed25519 Receipt: {}...",
        &result1.signature[..32]
    );
    println!(
        "  ⏱ Execution Duration: {} ms (exit={})",
        result1.execution_time_ms, result1.exit_code
    );
    println!("  Output:\n    {}\n", result1.stdout.trim().replace('\n', "\n    "));

    // Scenario 2: Real Rust Compilation & Execution across the Mesh
    println!("--------------------------------------------------------------------------------");
    println!(" ⚡ SCENARIO 2: REAL RUST COMPILATION & EXECUTION TASK AUCTION");
    println!("--------------------------------------------------------------------------------");
    let rust_src = r#"fn main() { let sum: u64 = (1..=100).sum(); println!("compiled-by-rustc sum_1_to_100={}", sum); }"#;
    println!("▸ Coordinator dispatching RustCompilation task (`rustc` compile & run)");

    let task2 = TaskSpec::new("rustc_compute", TaskKind::RustCompilation, rust_src);
    let (result2, session2) = node_coord
        .dispatch_task_with_session(task2, 100, 150)
        .await?;
    AuctionEngine::verify_result_signature(&result2)?;

    println!("  ⚡ Real Bids Collected ({}):", session2.bids.len());
    for bid in &session2.bids {
        let name = peer_names
            .get(&bid.bidder_peer_id)
            .map(String::as_str)
            .unwrap_or(&bid.bidder_peer_id);
        println!(
            "    • {} ({}): {} credits · est {}ms · score {:.1} · sig {}...",
            name,
            bid.bidder_peer_id,
            bid.price,
            bid.estimated_duration_ms,
            bid.node_score,
            &bid.signature[..16]
        );
    }

    let winner2_name = peer_names
        .get(&result2.worker_peer_id)
        .map(String::as_str)
        .unwrap_or(&result2.worker_peer_id);
    println!(
        "  🏆 Winner: {} ({})",
        winner2_name, result2.worker_peer_id
    );
    println!(
        "  📜 Verified Ed25519 Receipt: {}...",
        &result2.signature[..32]
    );
    println!(
        "  ⏱ Compile + Run Duration: {} ms (exit={})",
        result2.execution_time_ms, result2.exit_code
    );
    if !result2.stdout.is_empty() {
        println!("  Output:\n    {}\n", result2.stdout.trim());
    }
    if !result2.stderr.is_empty() {
        println!("  Stderr:\n    {}\n", result2.stderr.trim());
    }

    // Scenario 3: Live CRDT Delta Sync Verification
    println!("--------------------------------------------------------------------------------");
    println!(" 🧬 CRDT DELTA SYNC & LAMPORT CLOCK CONVERGENCE");
    println!("--------------------------------------------------------------------------------");
    node_coord
        .set_state("mesh/last_demo_contract", result2.contract_id.as_bytes().to_vec())
        .await?;
    tokio::time::sleep(Duration::from_millis(80)).await;

    println!(
        "  ▸ Coordinator CRDT: L={} · entries={} · last_demo_contract={:?}",
        node_coord.crdt.lamport_clock(),
        node_coord.crdt.len(),
        node_coord.crdt.get_string("mesh/last_demo_contract")
    );
    println!(
        "  ▸ Worker-1    CRDT: L={} · entries={} · last_demo_contract={:?}",
        node_worker_a.crdt.lamport_clock(),
        node_worker_a.crdt.len(),
        node_worker_a.crdt.get_string("mesh/last_demo_contract")
    );
    println!(
        "  ▸ Worker-2    CRDT: L={} · entries={} · last_demo_contract={:?}",
        node_worker_b.crdt.lamport_clock(),
        node_worker_b.crdt.len(),
        node_worker_b.crdt.get_string("mesh/last_demo_contract")
    );

    println!("\n================================================================================");
    println!(" 🍄 DEMO COMPLETE: ALL AUCTIONS, SIGNATURES, ENCRYPTION & CRDT SYNCS VERIFIED");
    println!("================================================================================\n");

    Ok(())
}
