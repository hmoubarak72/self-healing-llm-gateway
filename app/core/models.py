from __future__ import annotations

from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field


class ErrorCategory(str, Enum):
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    SERVER_ERROR = "server_error"
    CONTENT_FILTER = "content_filter"
    AUTH_FAILURE = "auth_failure"
    CLIENT_ERROR = "client_error"
    UNKNOWN = "unknown"


INFRASTRUCTURE_ERRORS = {
    ErrorCategory.RATE_LIMIT,
    ErrorCategory.TIMEOUT,
    ErrorCategory.SERVER_ERROR,
    ErrorCategory.AUTH_FAILURE,
}

RETRYABLE_ERRORS = {
    ErrorCategory.RATE_LIMIT,
    ErrorCategory.TIMEOUT,
    ErrorCategory.SERVER_ERROR,
}


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


CIRCUIT_STATE_NUMERIC = {
    CircuitState.CLOSED: 0,
    CircuitState.HALF_OPEN: 1,
    CircuitState.OPEN: 2,
}


class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "tool"]
    content: str | list[dict[str, Any]]
    name: str | None = None


class GatewayMetadata(BaseModel):
    tenant_id: str | None = None
    feature_name: str | None = None
    request_id: str | None = None
    request_class: str = "low_latency_classification"
    workload_type: Literal["interactive", "deferrable"] | None = None
    idempotency_key: str | None = None


class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage]
    temperature: float | None = 0.7
    max_tokens: int | None = None
    stream: bool = False
    top_p: float | None = None
    stop: str | list[str] | None = None
    metadata: GatewayMetadata | None = None
    tenant_id: str | None = None
    feature_name: str | None = None
    request_id: str | None = None
    request_class: str | None = None
    workload_type: Literal["interactive", "deferrable"] | None = None
    idempotency_key: str | None = None


class ResolvedAttribution(BaseModel):
    tenant_id: str
    feature_name: str
    request_id: str
    request_class: str
    workload_type: Literal["interactive", "deferrable"]
    idempotency_key: str | None = None


class UsageInfo(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChatChoiceMessage(BaseModel):
    role: str = "assistant"
    content: str


class ChatChoice(BaseModel):
    index: int = 0
    message: ChatChoiceMessage
    finish_reason: str = "stop"


class GatewayResponseMetadata(BaseModel):
    provider: str
    routed_model: str
    request_id: str
    tenant_id: str
    feature_name: str
    request_class: str
    workload_type: str
    estimated_cost_usd: float
    latency_ms: float
    retry_count: int = 0
    failover_count: int = 0
    hedged: bool = False
    circuit_state: str = "CLOSED"


class ChatCompletionResponse(BaseModel):
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: list[ChatChoice]
    usage: UsageInfo
    gateway_metadata: GatewayResponseMetadata | None = None


class QueuedCompletionResponse(BaseModel):
    status: Literal["queued"] = "queued"
    request_id: str
    idempotency_key: str
    tenant_id: str
    feature_name: str
    request_class: str
    queue_position: int
    status_url: str
    message: str = "All eligible providers are currently degraded; request queued for resilient retry."


class ChaosRule(BaseModel):
    provider: str
    latency_ms: int = Field(default=0, ge=0, le=60000)
    error_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    status_code: int = Field(default=500)
    timeout: bool = False
    duration_seconds: int = Field(default=60, ge=1, le=86400)
