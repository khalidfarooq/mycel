# 🍄 MYCEL

```text
  __  ____   ______ _____ _      
 |  \/  \ \ / / ___| ____| |     
 | |\/| |\ V / |   |  _| | |     
 | |  | | | || |___| |___| |___  
 |_|  |_| |_| \____|_____|_____| 
```

> **The Sovereign, Zero-Cloud P2P Mesh Network & Compute Exchange for Autonomous Edge Agents**  
> *Zero Servers. Zero Cloud APIs. Zero Centralized Orchestration. 100% Peer-to-Peer.*

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue.svg)](#)
[![Rust 1.83+](https://img.shields.io/badge/rust-1.83%2B-orange.svg)](#)
[![Architecture: Local-First](https://img.shields.io/badge/Architecture-Local--First-brightgreen.svg)](#)

---

## 🎬 Terminal Simulation: 3-Node Autonomous Mesh & Compute Auction

![Mycel Terminal Simulation Demo](assets/demo.gif)

---

## ⚡ Key Highlights

- 🌐 **Zero-Config P2P Discovery:** Nodes discover neighbors over local network via mDNS & QUIC in under 50ms.
- ⚡ **The Spore Reverse-Auction Protocol:** Tasks are dynamically auctioned to neighboring idle nodes with optimal hardware headroom.
- 🧬 **Conflict-Free Replicated State:** Distributed delta-CRDT memory ensures zero merge conflicts even after network partitions.
- 🔐 **Cryptographic Provenance:** Every peer identity, task bid, and execution receipt is Ed25519 signed and verifiable.

---

## 🏗️ Architecture

```mermaid
flowchart TD
    subgraph Agent["Autonomous AI Agent"]
        LLM["Agent Reasoning Engine"]
    end

    subgraph NodeA["Node A: MacBook M3 (Coordinator)"]
        CRYPTO_A["Ed25519 Identity"]
        AUCTION_A["Spore Reverse-Auction Engine"]
        CRDT_A["Delta-CRDT Distributed State"]
        NET_A["Mesh Swarm Layer"]
    end

    subgraph NodeB["Node B: Linux Workstation (Worker)"]
        EXEC_B["Sandboxed Task Runner"]
        CRYPTO_B["Ed25519 Signer & Receipt Generator"]
    end

    subgraph NodeC["Node C: Mac Mini (Edge)"]
        SLM_C["Local SLM Inference Engine"]
    end

    LLM <-->|Dispatch Task| AUCTION_A
    AUCTION_A <-->|Broadcast Task RFQ| NET_A
    NET_A <-->|Encrypted Wire| NodeB
    NET_A <-->|Encrypted Wire| NodeC
    NodeB -->|Signed Work Contract Receipt| AUCTION_A
    AUCTION_A -->|Update State| CRDT_A
```

---

## 📦 Workspace Crates

- **`mycel-proto`:** Protocol wire formats, task specifications, and hardware descriptors.
- **`mycel-crypto`:** Ed25519 node identities, cryptographic signatures, and SHA256 integrity proofs.
- **`mycel-net`:** Peer discovery, length-delimited TCP framing, mutual handshakes, and pub/sub channels.
- **`mycel-auction`:** The Spore reverse-auction market state machine (Auctioneer and Bidder roles).
- **`mycel-crdt`:** Partition-tolerant distributed memory with Lamport clocks.
- **`mycel-node`:** Core node daemon runtime, sandboxed task executor, and event routing loop.
- **`mycel-cli`:** Developer CLI and multi-node interactive simulation runner.

---

## 🚀 Quickstart

### 1. Build
```bash
git clone https://github.com/khalidfarooq/mycel.git
cd mycel
cargo build --release
```

### 2. Run the Multi-Node Cluster Simulation
```bash
cargo run -p mycel-cli -- demo
```

### 3. Launch the Interactive Real-Time TUI Dashboard
Experience live cluster topology, Spore auction tickers, and CRDT metrics:
```bash
cargo run -p mycel-cli -- tui
```

### 4. Initialize Node Identity
```bash
./target/release/mycel init
```

### 5. Dispatch a Task to the Mesh
```bash
./target/release/mycel dispatch --cmd "cargo test --workspace" --kind test --budget 100
```

---

## 🧪 Testing

```bash
cargo test --workspace
```
