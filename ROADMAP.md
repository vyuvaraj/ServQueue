# ServQueue Roadmap

This document outlines the planned evolutionary stages of **ServQueue** to evolve from a lightweight local broker into a distributed, high-performance messaging platform with inline compute capabilities.

---

## Phase 1: Core Foundation & WASM Integration (Completed)
- [x] **Thread-safe Pub/Sub engine**: Core routing structure mapping topics to active subscription channels.
- [x] **STOMP TCP Server**: Wire protocol parsing (`CONNECT`, `SUBSCRIBE`, `SEND`) for direct compatibility with Serv-lang.
- [x] **HTTP Management API**: Control endpoints for manual publishing and WASM filter attachment.
- [x] **WASM Sandbox Integration**: Wazero-based runner that passes messages through compiled `.wasm` modules.



---

## Phase 2: Production Observability, Security & Performance
- [x] **TLS Support**: Enable native TLS encryption for both the TCP STOMP server and the HTTP Management interface.
- [x] **Security Authentication**: Implement basic/token-based auth for HTTP APIs and username/passcodes in STOMP headers (`login`/`passcode` fields).
- [x] **OpenTelemetry Metrics & Tracing**: Instrument the engine using standard OTel APIs, tracking broker throughput, message latencies, WASM transform execution times, and trace spans.
- [x] **Module Caching**: Maintain compiled modules in-memory in `wazero` to eliminate instantiation latency.
- [x] **Direct Memory Passing**: Transition stdin/stdout pipes to guest-allocated shared memory buffers to reduce virtualization and allocation overhead.

---

## Phase 3: Cluster Consensus & Distributed Replication
- [x] **Raft-backed Clustering**: Implement Hashicorp Raft to replicate topic definitions and registered transforms across a 3-node cluster.
- [x] **Partitioned Queues**: Support message partitioning based on routing keys.
- [x] **High Availability**: Dynamic subscriber re-routing when a broker node drops.

---

## Phase 4: ServStore Tiered Storage (Infinite Backlog Retention)
- [x] **Write-Ahead Log (WAL)**: Record hot incoming messages to a local disk WAL.
- [x] **Cold Data Offloading**: Automatically roll WAL segments into structured segment files and upload them to `ServStore` / S3.
- [x] **Log Replay**: Enable client replay requests (e.g., `replay?since=timestamp`), pulling cold segments back from S3.

---

## Phase 5: Deep Ecosystem Integration
- [x] **Serv-lang Dedicated Protocol Driver**: Expand `runtime/broker.go` with a dedicated `servqueue://` driver that supports natively uploading WASM binaries, custom authentication schemas, and advanced queue options directly from `.srv` code.
- [x] **ServConsole Integration**: Feed broker throughput, active subscriptions, and WASM performance stats directly to the central Serv dashboard.
- [x] **Auto trace propagation**: Automatically pass trace context seamlessly into the WASM transform runtime environments.


