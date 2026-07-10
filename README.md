# ServQueue

```bash
docker run -p 8082:8082 -p 61613:61613 ghcr.io/vyuvaraj/servqueue:latest
```

`ServQueue` is a lightweight, distributed-ready message broker tailored for the **Serv** ecosystem. Its primary differentiating feature is **Compute-in-Queue** (Native WASM Stream Processing): the ability to run lightweight, compiled WebAssembly (WASI) modules inline inside the messaging pipeline to filter, enrich, or transform payloads dynamically before they reach subscribers.

---

## Key Features

* **WASM Transform Engine**: Leverage a sandboxed, pure-Go WASM runtime (`wazero`) to execute inline stream processing filters on topics.
* **STOMP Protocol Server**: Built-in TCP endpoint (`tcp://localhost:61613`) supporting standard STOMP subscription frames (`CONNECT`, `SUBSCRIBE`, `SEND`, `DISCONNECT`).
* **HTTP REST API**: Publish messages, subscribe, clear configurations, and query stats over HTTP (`http://localhost:8082`).
* **Telemetry & Context**: Out-of-the-box support for distributed trace propagation and execution logging.

---

## Project Structure

```
ServQueue/
├── pkg/
│   ├── broker/
│   │   ├── engine.go     # Message dispatch, subscriber routing, & transform hooks
│   │   └── wasm.go       # Wazero integration for WASI execution sandboxing
│   ├── stomp/
│   │   └── server.go     # STOMP protocol frame decoder/encoder & TCP server
│   └── web/
│       └── server.go     # HTTP JSON administration & publish endpoints
├── main.go               # Entrypoint & bootstrap configuration
├── ROADMAP.md            # Feature planning and progression tracker
└── README.md             # This documentation
```

---

## Quick Start

### 1. Build and Run
Ensure you have Go installed, then compile and run:
```bash
go build -o servqueue.exe main.go
./servqueue.exe
```
* The **STOMP TCP Server** listens on `:61613`
* The **HTTP Management API** listens on `:8082`

### 2. HTTP Admin API Usage

#### Publish a Message
```bash
curl -X POST http://localhost:8082/api/publish \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "payload": "hello world"}'
```

#### Register a WASM Transformation Module
Register a compiled `.wasm` file to automatically process all messages sent to a specific topic before delivery:
```bash
curl -X POST http://localhost:8082/api/topics/orders/transform \
  --data-binary @my_transform.wasm
```

#### Get Broker Stats
```bash
curl http://localhost:8082/api/stats
```

---

## Verification

Run the integration test suite:
```bash
go test ./... -v
```
