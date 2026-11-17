# Data-Path Architecture

1. **API Boundary & Attribution (`app/api/routes.py`, `app/utils/attribution.py`)**: Enforces strict precedence (`Headers > Top-Level Payload > Nested Metadata`) for `tenant_id`, `feature_name`, and `request_id`, returning `HTTP 400` when missing.
2. **Distributed Idempotency (`app/queueing/idempotency.py`)**: Uses Redis `SET NX` locks before dispatching upstream calls so duplicate client retries never cause duplicate downstream LLM completions or tenant double-billing.
3. **Sliding-Window Telemetry (`app/telemetry/store.py`)**: Aggregates per-provider counters and latency histogram buckets in 5-second epoch-aligned Redis hashes. Excludes `content_filter` and `client_error` from infrastructure degradation metrics.
4. **Distributed Circuit Breaker (`app/resilience/circuit_breaker.py`)**: Uses an atomic Redis Lua compare-and-set script to transition `CLOSED -> OPEN -> HALF_OPEN -> CLOSED` safely across multiple gateway replicas.
5. **Class-Aware Routing & Hedging (`app/routing/router.py`)**: Routes each `request_class` through its own provider preference chain and dispatches speculative hedged requests when primary latency crosses `threshold_ms`.
6. **Resilient Deferrable Queue (`app/queueing/worker.py`)**: Pushes `deferrable` requests into a Redis Sorted Set (`ZSET`) with exponential backoff + jitter during multi-provider outages, with dead-letter handling on deadline or max-retry exhaustion.
