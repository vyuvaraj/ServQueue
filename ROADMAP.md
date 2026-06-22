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

---

## Phase 6: Enterprise Features & Advanced Queueing ⚠️ High Priority
- [x] **Dead Letter Queues (DLQ)**: Automatically route messages failing WASM transformations or client acknowledgments after max-retries to a dedicated `.dlq` topic. *Critical reliability gap — failed transforms currently drop messages silently.*
- [x] **Delayed & Scheduled Messages**: Support publishing messages with a delayed delivery parameter (storing in a timed-wheel memory queue).
- [x] **Message Deduplication**: Deduplicate incoming publishes within a configured time-window based on unique message IDs to enable idempotent at-least-once delivery.

---

## Phase 7: Serv-verse Infrastructure Integrations
- [x] **ServGate API Gateway Webhook Triggers**: Support registering webhooks in `ServGate` that publish directly to `ServQueue` topics on incoming HTTP events.
- [ ] **ServConsole Unified Control Plane**: Expose complete topic administration, WAL inspection, and WASM performance debug panels directly in the central dashboard.
- [x] **Dynamic WASM hot-swap without dropping connections**: Support uploading new WASM transform modules via the console without dropping active subscriber TCP STOMP connections.

---

## Phase 8: Operational Hardening & Developer Experience (Proposed — Q3 2026)

| # | Item | Effort | Description | Status |
|---|------|--------|-------------|--------|
| 8.1 | **Standardized `/healthz` and `/readyz` endpoints** | Small | Health and readiness probes for k8s liveness checks and ServConsole status aggregation. | [x] |
| 8.2 | **Graceful shutdown on SIGTERM** | Small | Drain in-flight STOMP messages, flush WAL, and close connections cleanly before exit. Required for rolling updates. | [x] |
| 8.3 | **Standardized error response contract** | Small | All HTTP API errors return `{"error": "msg", "code": "ERR_CODE", "trace_id": "..."}` — consistent across ecosystem. | [x] |
| 8.4 | **API versioning (`/api/v1/`)** | Small | Version the management API before breaking changes accumulate. | [x] |
| 8.5 | **Rate limiting on publish endpoint** | Small | Protect `POST /api/publish` against flooding — currently unthrottled. | [ ] |
| 8.6 | **CI/CD pipeline (GitHub Actions)** | Small | Automated build, test, and format checks on every PR. Currently missing. | [x] |
| 8.7 | **WebSocket push for real-time metrics** | Medium | Push live throughput, subscriber counts, and WASM execution stats to ServConsole via WebSocket. | [ ] |
| 8.8 | **Consumer group support** | Large | Multiple subscribers in a consumer group with partition assignment — enables horizontal scaling of message consumers. | [x] |
| 8.9 | **Message priority levels** | Medium | Support priority tiers on publish so high-priority messages are delivered ahead of low-priority ones. | [ ] |

---

## Phase 9: Next-Level Message Broker (Proposed — Q4 2026+)

These items take ServQueue from a lightweight broker to a **category-defining event streaming platform** — competing with Kafka, Pulsar, and AWS Kinesis while maintaining Serv's simplicity.

| # | Item | Effort | Description | Status |
|---|------|--------|-------------|--------|
| 9.1 | **Exactly-once delivery semantics** | Large | Idempotent producer IDs + transactional message batches. Guarantees no duplicates and no loss even across broker restarts. The gold standard for financial/ordering systems. | [ ] |
| 9.2 | **Schema registry & validation** | Medium | Attach Avro/JSON Schema to topics. Reject non-conforming publishes at the broker. Auto-evolve schemas with compatibility checks. | [ ] |
| 9.3 | **Topic compaction** | Medium | Retain only the latest message per key within a topic — useful for changelog/state topics. Similar to Kafka log compaction. | [ ] |
| 9.4 | **Multi-tenant topic isolation** | Medium | Namespace-scoped topics with independent quotas, rate limits, and RBAC policies per tenant. Enables shared cluster deployment. | [ ] |
| 9.5 | **Stream processing DSL** | Large | Built-in windowed aggregations, joins, and filters expressed in `.srv` syntax: `stream "orders" |> filter(o => o.total > 100) |> window(5m) |> count() |> publish("high-value-orders")`. | [ ] |
| 9.6 | **Message replay with offset management** | Medium | Named consumer offsets with commit/seek semantics. Replay from any point in the WAL by offset or timestamp without re-creating subscriptions. | [ ] |
| 9.7 | **Fan-out patterns (broadcast + routing keys)** | Medium | Support topic routing patterns: `orders.*` (wildcard), `orders.us.#` (multi-level). Enables flexible pub/sub topologies without multiple topics. | [ ] |
| 9.8 | **Backpressure & flow control** | Medium | When consumers are slow, apply configurable backpressure: pause publishes, buffer to disk, or reject with `429`. Prevents unbounded memory growth. | [ ] |
| 9.9 | **Cross-cluster mirroring** | Large | Replicate topics between geographically separate ServQueue clusters for disaster recovery and multi-region active-active setups. | [ ] |
| 9.10 | **Message tracing (end-to-end)** | Medium | Track a message from publish through every WASM transform, DLQ redirect, and consumer ack — visualizable in ServConsole as a message journey timeline. | [ ] |
| 9.11 | **WASM transform marketplace** | Medium | Install community or private transforms via `servqueue install <name>` resolving from ServRegistry. Pre-built transforms: JSON→Protobuf, PII masking, enrichment. | [ ] |
| 9.12 | **Message TTL & expiration** | Small | Per-topic or per-message TTL. Expired messages are automatically moved to DLQ or purged. Essential for time-sensitive event processing. | [ ] |
| 9.13 | **Admin CLI (`servqueue` binary)** | Medium | Terminal client supporting `topics list`, `topics create`, `publish`, `consume`, `offsets`, `transforms list/upload`, and cluster management. | [ ] |
| 9.14 | **Observability dashboard (built-in)** | Medium | Embedded lightweight web UI (like ServStore's console) showing topic throughput, consumer lag, WASM execution stats, and DLQ depth without needing ServConsole. | [ ] |

---

## Phase 10: Differentiating Factors — What No Other Broker Offers (Strategic)

These create a **moat** around ServQueue — capabilities that Kafka, RabbitMQ, NATS, and Pulsar cannot replicate without fundamental architecture changes.

| # | Item | Effort | Description | Why Nobody Else Can Do This |
|---|------|--------|-------------|----------------------------|
| 10.1 | **Compute-in-queue (WASM transforms)** | Already Done | Messages pass through sandboxed WASM functions INSIDE the broker — no external stream processors needed. Filter, enrich, transform, or route messages at broker speed. | No other broker runs arbitrary user code inside the message path. Kafka needs Kafka Streams (separate JVM). RabbitMQ needs a Shovel or external consumer. |
| 10.2 | **Language-native protocol driver** | Already Done | Serv-lang's `broker "servqueue://host"` compiles to a zero-config STOMP client with auto-auth, trace propagation, and typed message schemas — all generated by the compiler. | Tight compiler→broker integration. Other brokers need SDK libraries manually imported and configured. |
| 10.3 | **AI-powered message routing** | Large | Route messages to different consumers based on semantic content: `subscribe "support-tickets" where ai.classify(msg) == "urgent"`. The broker evaluates an embedding model on each message to make routing decisions. | WASM sandbox can run ONNX inference models. No other broker has an embedded ML runtime. |
| 10.4 | **Schema-on-write with WASM validation** | Medium | Attach a WASM validator to a topic — invalid messages are rejected at publish time with a structured error. Zero-latency schema enforcement without a separate schema registry service. | WASM execution at the ingestion point. Kafka's schema registry is a separate service that only validates producers, not at the broker level. |
| 10.5 | **Tiered storage with ServStore (infinite retention)** | Already Done | Cold messages automatically offload to ServStore (S3-compatible) and transparently replay on request. Infinite retention at object-storage cost, not SSD cost. | Integrated with the ecosystem's storage engine. Kafka Tiered Storage requires separate S3 config; Pulsar needs BookKeeper. ServQueue uses its own storage engine natively. |
| 10.6 | **Transform pipeline chaining** | Medium | Chain multiple WASM transforms sequentially: `topic "raw" → validate.wasm → enrich.wasm → route.wasm → topic "processed"`. Declarative pipeline without external orchestration. | Multi-stage WASM pipeline inside the broker. No other broker supports composable transform chains. |
| 10.7 | **Gateway-integrated event sourcing** | Already Done | ServGate routes can publish to ServQueue topics on every request — the gateway becomes an event sourcing entry point. HTTP → Event in one hop, one config line. | Tight ServGate↔ServQueue integration. Requires separate webhook relay services with Kafka/RabbitMQ. |
| 10.8 | **Single-binary deployment** | Already Done | The entire broker (STOMP server + HTTP API + WASM runtime + WAL + clustering) compiles to a single Go binary. No JVM, no Erlang VM, no external dependencies. Deploy by copying one file. | Pure Go. Kafka = JVM + ZooKeeper. RabbitMQ = Erlang. Pulsar = JVM + BookKeeper. ServQueue = one file. |
| 10.9 | **Trace context propagation through transforms** | Already Done | OTel trace context flows from publisher → through every WASM transform → to subscriber. The full message journey is a single distributed trace. | Most brokers lose trace context between producer and consumer. ServQueue maintains it through arbitrary transform stages. |
| 10.10 | **Real-time WASM hot-swap** | Already Done | Swap transform modules without disconnecting subscribers or dropping messages. Active connections continue with the new logic on the next message. | Atomic module replacement at the message boundary. Kafka Streams requires rebalancing; Flink requires savepoint + restart. |

> See [UNIFIED_ROADMAP.md](../UNIFIED_ROADMAP.md) for the full ecosystem priority matrix and architectural recommendations.




