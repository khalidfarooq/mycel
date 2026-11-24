use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Row, Table},
    Terminal,
};
use tokio::sync::Mutex;

use mycel_crypto::NodeIdentity;
use mycel_net::NetworkConfig;
use mycel_node::NodeDaemon;
use mycel_proto::{HardwareProfile, TaskKind, TaskSpec};

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen, DisableMouseCapture);
    }
}

pub struct TuiState {
    pub log_events: Vec<String>,
    pub total_auctions: u64,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub handshake_ms: f64,
}

fn format_ram(ram_mb: u64) -> String {
    if ram_mb >= 1024 {
        format!("{:.1} GB", ram_mb as f64 / 1024.0)
    } else {
        format!("{} MB", ram_mb)
    }
}

fn format_accel(hw: &HardwareProfile) -> &'static str {
    if hw.has_metal {
        "Metal"
    } else if hw.has_cuda {
        "CUDA"
    } else {
        "CPU"
    }
}

pub async fn run_dashboard(max_seconds: Option<u64>) -> Result<(), Box<dyn std::error::Error>> {
    let mut peer_names: HashMap<String, String> = HashMap::new();

    let id_coord = NodeIdentity::generate();
    let hw_coord = HardwareProfile::detect();
    let node_coord = NodeDaemon::start(
        id_coord.clone(),
        "coord",
        hw_coord.clone(),
        NetworkConfig::default(),
    )
    .await?;
    peer_names.insert(id_coord.peer_id().to_string(), "coord".into());

    let id_worker_a = NodeIdentity::generate();
    let hw_worker_a = HardwareProfile::detect();
    let node_worker_a = NodeDaemon::start(
        id_worker_a.clone(),
        "worker-1",
        hw_worker_a.clone(),
        NetworkConfig::default(),
    )
    .await?;
    peer_names.insert(id_worker_a.peer_id().to_string(), "worker-1".into());

    let id_worker_b = NodeIdentity::generate();
    let hw_worker_b = HardwareProfile::detect();
    let node_worker_b = NodeDaemon::start(
        id_worker_b.clone(),
        "worker-2",
        hw_worker_b.clone(),
        NetworkConfig::default(),
    )
    .await?;
    peer_names.insert(id_worker_b.peer_id().to_string(), "worker-2".into());

    let hs_start = Instant::now();
    let _ = node_worker_a
        .network
        .connect_peer(node_coord.network.actual_tcp_addr)
        .await;
    let _ = node_worker_b
        .network
        .connect_peer(node_coord.network.actual_tcp_addr)
        .await;
    let handshake_ms = hs_start.elapsed().as_secs_f64() * 1000.0;

    // Wait briefly for PeerDiscovered events to populate CRDT peer table
    let wait_deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < wait_deadline {
        if node_coord.list_peers().len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    let peer_names = Arc::new(peer_names);
    let state = Arc::new(Mutex::new(TuiState {
        log_events: vec![
            format!(
                "✓ Mesh ready: Ed25519 + X25519 + ChaCha20-Poly1305 ({:.2}ms)",
                handshake_ms
            ),
            format!(
                "▸ Local Coordinator: {} ({} cores · {})",
                node_coord.node_name,
                hw_coord.cpu_cores,
                format_ram(hw_coord.ram_mb)
            ),
            format!(
                "▸ Connected Workers: {} peers active over encrypted TCP",
                node_coord.network.connected_peer_count().await
            ),
            "⚡ Spore reverse-auction engine ready — press [d], [r], or [s]".into(),
        ],
        total_auctions: 0,
        completed_tasks: 0,
        failed_tasks: 0,
        handshake_ms,
    }));

    enable_raw_mode()?;
    let _guard = TerminalGuard;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();
    let start_time = Instant::now();

    loop {
        if let Some(secs) = max_seconds {
            if start_time.elapsed() >= Duration::from_secs(secs) {
                break;
            }
        }

        let active_peer_count = node_coord.network.connected_peer_count().await;
        let peers = node_coord.list_peers();
        let lamport_clock = node_coord.crdt.lamport_clock();
        let crdt_entries = node_coord.crdt.len();

        {
            let s = state.lock().await;
            terminal.draw(|f| {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Min(14),
                        Constraint::Length(3),
                    ])
                    .split(f.size());

                let header_text = vec![Line::from(vec![
                    Span::styled(
                        " 🍄 MYCEL ",
                        Style::default()
                            .fg(Color::LightCyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("— Sovereign P2P Mesh & Spore Exchange  "),
                    Span::styled(
                        format!("● {} REMOTE PEERS", active_peer_count),
                        Style::default()
                            .fg(Color::LightGreen)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!(
                        "  [X25519+ChaCha20 · HS {:.2}ms]",
                        s.handshake_ms
                    )),
                ])];
                let header = Paragraph::new(header_text).block(
                    Block::default()
                        .borders(Borders::ALL)
                        .style(Style::default().fg(Color::DarkGray)),
                );
                f.render_widget(header, chunks[0]);

                let body_chunks = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
                    .split(chunks[1]);

                let left_chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(9), Constraint::Min(6)])
                    .split(body_chunks[0]);

                let mut total_cores = node_coord.hardware.cpu_cores;
                let mut total_ram_mb = node_coord.hardware.ram_mb;
                let mut rows = vec![Row::new(vec![
                    node_coord.node_name.clone(),
                    "Coordinator".to_string(),
                    format!("{} Cores", node_coord.hardware.cpu_cores),
                    format_ram(node_coord.hardware.ram_mb),
                    format_accel(&node_coord.hardware).to_string(),
                    format!("{:.2}", node_coord.hardware.load_average),
                    "ONLINE".to_string(),
                ])
                .style(Style::default().fg(Color::Cyan))];

                let mut sorted_peers = peers.clone();
                sorted_peers.sort_by(|a, b| a.node_name.cmp(&b.node_name));
                for p in &sorted_peers {
                    total_cores += p.hardware.cpu_cores;
                    total_ram_mb += p.hardware.ram_mb;
                    rows.push(
                        Row::new(vec![
                            p.node_name.clone(),
                            "Worker".to_string(),
                            format!("{} Cores", p.hardware.cpu_cores),
                            format_ram(p.hardware.ram_mb),
                            format_accel(&p.hardware).to_string(),
                            format!("{:.2}", p.hardware.load_average),
                            "ONLINE".to_string(),
                        ])
                        .style(Style::default().fg(Color::Green)),
                    );
                }

                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(14),
                        Constraint::Length(12),
                        Constraint::Length(10),
                        Constraint::Length(9),
                        Constraint::Length(7),
                        Constraint::Length(7),
                        Constraint::Length(8),
                    ],
                )
                .header(
                    Row::new(vec![
                        "Node Name",
                        "Role",
                        "Cores",
                        "RAM",
                        "Accel",
                        "Load",
                        "Status",
                    ])
                    .style(
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ),
                )
                .block(
                    Block::default()
                        .title(" 🖥️ Mesh Cluster Topology (Live Host Metrics) ")
                        .borders(Borders::ALL),
                );
                f.render_widget(table, left_chunks[0]);

                let node_count = 1 + sorted_peers.len();
                let stats_text = vec![
                    Line::from(format!(
                        "  ▸ Total Compute:      {} Logical Cores across {} Nodes",
                        total_cores, node_count
                    )),
                    Line::from(format!(
                        "  ▸ Total Memory:       {} across {} Nodes",
                        format_ram(total_ram_mb),
                        node_count
                    )),
                    Line::from(format!(
                        "  ▸ Auctions Settled:   {} ok / {} failed ({} total)",
                        s.completed_tasks, s.failed_tasks, s.total_auctions
                    )),
                    Line::from(format!(
                        "  ▸ CRDT State:         L={} ({} synced entries)",
                        lamport_clock, crdt_entries
                    )),
                ];
                let stats_widget = Paragraph::new(stats_text).block(
                    Block::default()
                        .title(" 📊 Distributed Metrics & CRDT State ")
                        .borders(Borders::ALL),
                );
                f.render_widget(stats_widget, left_chunks[1]);

                let log_items: Vec<ListItem> = s
                    .log_events
                    .iter()
                    .rev()
                    .take(18)
                    .map(|msg| {
                        let color = if msg.contains("[RECEIPT]") || msg.contains("✓") {
                            Color::LightGreen
                        } else if msg.contains("[ERR]") {
                            Color::LightRed
                        } else if msg.contains("[RFQ]") || msg.contains("Spore") {
                            Color::LightYellow
                        } else if msg.contains("[BID]") || msg.contains("[AWARD]") {
                            Color::LightCyan
                        } else {
                            Color::Gray
                        };
                        ListItem::new(Line::from(vec![Span::styled(
                            msg.as_str(),
                            Style::default().fg(color),
                        )]))
                    })
                    .collect();

                let list = List::new(log_items).block(
                    Block::default()
                        .title(" ⚡ Live Spore Auction Stream ")
                        .borders(Borders::ALL),
                );
                f.render_widget(list, body_chunks[1]);

                let footer_text = vec![Line::from(vec![
                    Span::styled(
                        " [d] ",
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("Bash Probe  "),
                    Span::styled(
                        " [r] ",
                        Style::default()
                            .fg(Color::Green)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("Rust Compile  "),
                    Span::styled(
                        " [s] ",
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("Local SLM  "),
                    Span::styled(
                        " [c] ",
                        Style::default()
                            .fg(Color::Magenta)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("Clear  "),
                    Span::styled(
                        " [q] ",
                        Style::default()
                            .fg(Color::Red)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("Quit"),
                ])];
                let footer = Paragraph::new(footer_text).block(Block::default().borders(Borders::ALL));
                f.render_widget(footer, chunks[2]);
            })?;
        }

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Char('c') => {
                        let mut s = state.lock().await;
                        s.log_events.clear();
                        s.log_events.push("✓ Event ticker cleared".into());
                    }
                    KeyCode::Char('d') => {
                        spawn_tui_task(
                            state.clone(),
                            node_coord.clone(),
                            peer_names.clone(),
                            TaskSpec::new("host_probe", TaskKind::BashScript, "uname -ms && uptime"),
                        );
                    }
                    KeyCode::Char('r') => {
                        spawn_tui_task(
                            state.clone(),
                            node_coord.clone(),
                            peer_names.clone(),
                            TaskSpec::new(
                                "rustc_exec",
                                TaskKind::RustCompilation,
                                r#"fn main() { println!("rustc_mesh_ok={}", 21 * 2); }"#,
                            ),
                        );
                    }
                    KeyCode::Char('s') => {
                        spawn_tui_task(
                            state.clone(),
                            node_coord.clone(),
                            peer_names.clone(),
                            TaskSpec::new(
                                "slm_infer",
                                TaskKind::SlmInference,
                                "Summarize Mycel in one sentence.",
                            ),
                        );
                    }
                    _ => {}
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }

    drop(_guard);
    terminal.show_cursor()?;
    println!("✓ Mycel TUI exited gracefully.");
    Ok(())
}

fn spawn_tui_task(
    state: Arc<Mutex<TuiState>>,
    coord: Arc<NodeDaemon>,
    peer_names: Arc<HashMap<String, String>>,
    task: TaskSpec,
) {
    tokio::spawn(async move {
        let task_name = task.name.clone();
        let task_kind = format!("{:?}", task.kind);
        {
            let mut s = state.lock().await;
            s.log_events
                .push(format!("▸ [RFQ] Dispatched '{}' ({})", task_name, task_kind));
            s.total_auctions += 1;
        }

        match coord.dispatch_task_with_session(task, 100, 150).await {
            Ok((result, session)) => {
                let mut s = state.lock().await;
                for bid in &session.bids {
                    let bname = peer_names
                        .get(&bid.bidder_peer_id)
                        .map(String::as_str)
                        .unwrap_or(&bid.bidder_peer_id);
                    s.log_events.push(format!(
                        "⚡ [BID] {}: {} cr (est {}ms, score {:.1})",
                        bname, bid.price, bid.estimated_duration_ms, bid.node_score
                    ));
                }
                let winner = peer_names
                    .get(&result.worker_peer_id)
                    .map(String::as_str)
                    .unwrap_or(&result.worker_peer_id);
                s.log_events.push(format!(
                    "🏆 [AWARD] {} won contract ({}ms)",
                    winner, result.execution_time_ms
                ));
                if result.success {
                    s.completed_tasks += 1;
                    s.log_events
                        .push(format!("✓ [RECEIPT] {}", result.stdout.trim()));
                } else {
                    s.failed_tasks += 1;
                    let err_msg = if result.stderr.trim().is_empty() {
                        format!("exit={}", result.exit_code)
                    } else {
                        result.stderr.trim().to_string()
                    };
                    s.log_events.push(format!("✗ [ERR] {}", err_msg));
                }
            }
            Err(e) => {
                let mut s = state.lock().await;
                s.failed_tasks += 1;
                s.log_events.push(format!("✗ [ERR] Auction failed: {}", e));
            }
        }
    });
}
