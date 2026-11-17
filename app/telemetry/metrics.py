from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

REQUESTS_TOTAL = Counter(
    "llmgw_requests_total",
    "Total gateway completion requests by attribution and outcome",
    ["provider", "model", "tenant_id", "feature_name", "request_class", "workload_type", "outcome"],
)

REQUEST_LATENCY_SECONDS = Histogram(
    "llmgw_request_latency_seconds",
    "End-to-end provider call latency in seconds",
    ["provider", "model", "request_class"],
    buckets=(0.05, 0.1, 0.25, 0.5, 0.8, 1.0, 1.5, 2.5, 5.0, 10.0, 20.0),
)

TOKENS_TOTAL = Counter(
    "llmgw_tokens_total",
    "Total tokens processed by tenant, feature, provider, model, and token_type",
    ["tenant_id", "feature_name", "provider", "model", "token_type"],
)

ESTIMATED_COST_USD_TOTAL = Counter(
    "llmgw_estimated_cost_usd_total",
    "Estimated USD cost by tenant, feature, provider, and model",
    ["tenant_id", "feature_name", "provider", "model"],
)

ERRORS_TOTAL = Counter(
    "llmgw_errors_total",
    "Total classified errors by provider and taxonomy category",
    ["provider", "error_type"],
)

CIRCUIT_BREAKER_STATE = Gauge(
    "llmgw_circuit_breaker_state",
    "Current circuit breaker state (0=CLOSED, 1=HALF_OPEN, 2=OPEN)",
    ["provider"],
)

CIRCUIT_BREAKER_TRANSITIONS_TOTAL = Counter(
    "llmgw_circuit_breaker_transitions_total",
    "Circuit breaker state transitions",
    ["provider", "from_state", "to_state"],
)

FAILOVERS_TOTAL = Counter(
    "llmgw_failovers_total",
    "Total provider failovers triggered",
    ["from_provider", "to_provider", "request_class"],
)

HEDGED_REQUESTS_TOTAL = Counter(
    "llmgw_hedged_requests_total",
    "Total hedged speculative requests dispatched and won",
    ["primary_provider", "secondary_provider", "winner_provider", "request_class"],
)

HEDGED_DUPLICATE_TOKENS_TOTAL = Counter(
    "llmgw_hedged_duplicate_tokens_total",
    "Duplicate tokens incurred from losing hedged speculative calls",
    ["provider", "request_class"],
)

QUEUE_DEPTH = Gauge(
    "llmgw_queue_depth",
    "Current number of deferrable requests waiting in Redis queue",
)

QUEUE_RETRIES_TOTAL = Counter(
    "llmgw_queue_retries_total",
    "Total queue worker retry attempts",
    ["request_class"],
)

QUEUE_DEAD_LETTERS_TOTAL = Counter(
    "llmgw_queue_dead_letters_total",
    "Total deferrable requests moved to dead-letter queue",
    ["request_class", "reason"],
)

QUEUE_PROCESSING_LATENCY_SECONDS = Histogram(
    "llmgw_queue_processing_latency_seconds",
    "Total time spent by deferrable requests in queue before completion",
    ["request_class"],
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 180.0, 300.0),
)

REDIS_OPERATION_LATENCY_SECONDS = Histogram(
    "llmgw_redis_operation_latency_seconds",
    "Latency of Redis telemetry/state operations",
    ["operation"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1),
)

REDIS_ERRORS_TOTAL = Counter(
    "llmgw_redis_errors_total",
    "Total Redis operational errors",
    ["operation"],
)
