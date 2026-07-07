# ServShared Library

ServShared is the shared utility and middleware library powering core functions across the Servverse ecosystem: ServGate, ServStore, ServQueue, ServConsole, and more.

## Features

### 🔑 Auth & JWKS Validation (`auth.go`, `jwks_test.go`)
- Multi-tenant JWT authorization parsing.
- Dynamic key rotation and verification using JSON Web Key Sets (JWKS).
- Caching token validators to optimize authentication checks.

### 🛡️ RBAC Authorization (`rbac.go`)
- Role-based and scope-based request guards.
- Policy enforcement middleware for endpoints.

### 🌐 Tenant Resource Isolation (`middleware.go`)
- Context-aware multitenancy bindings.
- Helpers to isolate topics (`IsolateTopic`), databases (`IsolateDBPool`), and storage buckets (`IsolateBucket`).

### 🔒 Mutual TLS helpers (`mtls.go`)
- Dynamic loading of client/server mTLS cert configurations.

### ⚡ Logging Sanitizer (`middleware.go`)
- Regex-based structured log sanitizer (`SanitizeLog`) that automatically redacts unquoted, double-quoted, and single-quoted tokens, credentials, and secrets from JSON/plaintext outputs before emission.

### 📊 OpenTelemetry Instrumentation (`otel.go`)
- Trace context propagation (`traceparent` header parsing).
- Trace log correlation and Prometheus metrics formatting.

### 🔒 Distributed Lock Manager Client (`lock_client.go`)
- `DistributedLocker` interface decoupling the registry implementation.
- `HTTPLockClient` talking to ServMesh's lock APIs.
- Mutual exclusion utility helpers (`WithLock`, `WithLockRetry`).
- Mock/test-friendly `NoOpLocker`.

