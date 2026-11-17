from __future__ import annotations

import json
from typing import Any
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class HedgeConfig(BaseModel):
    enabled: bool = False
    threshold_ms: int = 800


class RequestClassConfig(BaseModel):
    providers: list[str]
    hedge: HedgeConfig = Field(default_factory=HedgeConfig)
    workload_type: str = "interactive"


class ModelPricing(BaseModel):
    input_per_1k_usd: float
    output_per_1k_usd: float


class ProviderConfig(BaseModel):
    name: str
    litellm_prefix: str
    default_model: str
    timeout_seconds: float = 15.0
    enabled: bool = True


DEFAULT_PROVIDERS: dict[str, ProviderConfig] = {
    "openai": ProviderConfig(name="openai", litellm_prefix="openai", default_model="gpt-4o-mini", timeout_seconds=12.0),
    "anthropic": ProviderConfig(name="anthropic", litellm_prefix="anthropic", default_model="claude-3-5-haiku-latest", timeout_seconds=15.0),
    "gemini": ProviderConfig(name="gemini", litellm_prefix="gemini", default_model="gemini-2.0-flash", timeout_seconds=12.0),
}

DEFAULT_REQUEST_CLASSES: dict[str, RequestClassConfig] = {
    "low_latency_classification": RequestClassConfig(
        providers=["openai", "gemini", "anthropic"],
        hedge=HedgeConfig(enabled=True, threshold_ms=800),
        workload_type="interactive",
    ),
    "long_form_generation": RequestClassConfig(
        providers=["anthropic", "openai", "gemini"],
        hedge=HedgeConfig(enabled=False, threshold_ms=2000),
        workload_type="interactive",
    ),
    "batch_enrichment": RequestClassConfig(
        providers=["gemini", "openai", "anthropic"],
        hedge=HedgeConfig(enabled=False, threshold_ms=2000),
        workload_type="deferrable",
    ),
}

DEFAULT_MODEL_PRICING: dict[str, ModelPricing] = {
    "gpt-4o-mini": ModelPricing(input_per_1k_usd=0.00015, output_per_1k_usd=0.00060),
    "gpt-4o": ModelPricing(input_per_1k_usd=0.00250, output_per_1k_usd=0.01000),
    "claude-3-5-haiku-latest": ModelPricing(input_per_1k_usd=0.00080, output_per_1k_usd=0.00400),
    "claude-3-5-sonnet-latest": ModelPricing(input_per_1k_usd=0.00300, output_per_1k_usd=0.01500),
    "gemini-2.0-flash": ModelPricing(input_per_1k_usd=0.00010, output_per_1k_usd=0.00040),
    "gemini-1.5-pro": ModelPricing(input_per_1k_usd=0.00125, output_per_1k_usd=0.00500),
}

DEFAULT_MODEL_ALIASES: dict[str, dict[str, str]] = {
    "gpt-4o-mini": {"openai": "gpt-4o-mini", "anthropic": "claude-3-5-haiku-latest", "gemini": "gemini-2.0-flash"},
    "gpt-4o": {"openai": "gpt-4o", "anthropic": "claude-3-5-sonnet-latest", "gemini": "gemini-1.5-pro"},
    "claude-3-5-haiku-latest": {"anthropic": "claude-3-5-haiku-latest", "openai": "gpt-4o-mini", "gemini": "gemini-2.0-flash"},
    "claude-3-5-sonnet-latest": {"anthropic": "claude-3-5-sonnet-latest", "openai": "gpt-4o", "gemini": "gemini-1.5-pro"},
    "gemini-2.0-flash": {"gemini": "gemini-2.0-flash", "openai": "gpt-4o-mini", "anthropic": "claude-3-5-haiku-latest"},
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    gateway_host: str = "0.0.0.0"
    gateway_port: int = 8000
    gateway_log_level: str = "INFO"
    gateway_environment: str = "production"
    admin_api_key: str = "change-me-super-secret-admin-key"
    mock_providers_enabled: bool = True

    redis_url: str = "redis://localhost:6379/0"
    redis_key_prefix: str = "llmgw"

    openai_api_key: str | None = None
    anthropic_api_key: str | None = None
    gemini_api_key: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region_name: str = "us-east-1"

    telemetry_window_seconds: int = 60
    telemetry_bucket_seconds: int = 5

    cb_min_sample_size: int = 5
    cb_error_rate_threshold: float = 0.5
    cb_p95_latency_threshold_ms: float = 2500.0
    cb_open_duration_seconds: int = 20
    cb_half_open_canary_rate: float = 0.10
    cb_required_successes_to_close: int = 3
    cb_required_failures_to_reopen: int = 1

    queue_max_retries: int = 5
    queue_base_backoff_seconds: float = 1.0
    queue_max_backoff_seconds: float = 30.0
    queue_deadline_seconds: int = 300
    queue_poll_interval_seconds: float = 0.25
    idempotency_ttl_seconds: int = 3600
    idempotency_lock_ttl_seconds: int = 30

    custom_pricing_json: str | None = None
    custom_request_classes_json: str | None = None

    @property
    def providers(self) -> dict[str, ProviderConfig]:
        return DEFAULT_PROVIDERS

    @property
    def request_classes(self) -> dict[str, RequestClassConfig]:
        classes = dict(DEFAULT_REQUEST_CLASSES)
        if self.custom_request_classes_json:
            raw: dict[str, Any] = json.loads(self.custom_request_classes_json)
            for k, v in raw.items():
                classes[k] = RequestClassConfig(**v)
        return classes

    @property
    def model_pricing(self) -> dict[str, ModelPricing]:
        pricing = dict(DEFAULT_MODEL_PRICING)
        if self.custom_pricing_json:
            raw: dict[str, Any] = json.loads(self.custom_pricing_json)
            for k, v in raw.items():
                pricing[k] = ModelPricing(**v)
        return pricing

    @property
    def model_aliases(self) -> dict[str, dict[str, str]]:
        return DEFAULT_MODEL_ALIASES


def get_settings() -> Settings:
    return Settings()
