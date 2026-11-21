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
[![Rust 1.80+](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](#)
[![P2P Protocol: libp2p](https://img.shields.io/badge/P2P-libp2p-purple.svg)](#)
[![Local First](https://img.shields.io/badge/Architecture-Local--First-brightgreen.svg)](#)

---

## ⚡ What is Mycel?

Today's multi-agent architectures (LangGraph, CrewAI, AutoGen) are fundamentally fragile: they depend on centralized cloud coordinators, remote databases, and constant internet connectivity. When your Wi-Fi blinks or cloud APIs throttle, your entire agent workflow dies.

**Mycel** changes the paradigm:
- 🌐 **Zero-Config P2P Discovery:** Nodes discover each other over local Wi-Fi, LAN, or Bluetooth via **mDNS & QUIC** in under 50ms.
- ⚡ **The "Spore" Reverse-Auction Compute Protocol:** Nodes dynamically auction off compute-intensive tasks (tests, compilation, sandbox execution, local SLM inference) to neighboring idle nodes.
- 🧬 **Conflict-Free Shared Memory:** Distributed CRDT state replication ensures zero merge conflicts even after network partitions.
- 🔐 **Cryptographic Provenance:** Every peer identity, task bid, and execution result is cryptographically signed with Ed25519 keypairs.

---

## 🏗️ Architecture

```text
             +-------------------------------------------------------+
             |                  Autonomous AI Agent                  |
             +---------------------------+---------------------------+
                                         v
             +-------------------------------------------------------+
             |                      Mycel Node                       |
             |                                                       |
             |   [Ed25519 Crypto]  <--->  [Spore Auction Market]     |
             |   [CRDT Cluster State]  <--->  [libp2p Swarm Layer]   |
             +---------------------------+---------------------------+
                                         |
                     +-------------------+-------------------+
                     | Encrypted P2P Wire (Noise / QUIC)     |
                     v                                       v
        +-------------------------+             +-------------------------+
        |     Neighbor Node A     |             |     Neighbor Node B     |
        | (MacBook M3 Max / Metal)|             | (Linux Workstation / CPU|
        +-------------------------+             +-------------------------+
```

---

## 📦 Workspace Overview

- `mycel-proto`: Core protocol buffer and bincode serialization schemas for the Spore wire protocol.
- `mycel-crypto`: Ed25519 identity, signature generation/verification, and SHA256 integrity proofs.
- `mycel-net`: libp2p swarm engine with mDNS discovery, QUIC transport, and Gossipsub message routing.
- `mycel-auction`: The decentralized compute reverse-auction engine (RFQ -> Bid -> Work Contract -> Receipt).
- `mycel-crdt`: Delta-based conflict-free replicated state store for distributed agent memory.
- `mycel-node`: Integrated node runtime daemon and sandboxed execution engine.
- `mycel-cli`: Interactive CLI and terminal dashboard.

---

## 📜 Full Specifications

Read the complete engineering RFC in [PRD.md](./PRD.md).
