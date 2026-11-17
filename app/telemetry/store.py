from __future__ import annotations

import math
import time
from typing import Any
import redis.asyncio as redis

from app.core.models import ErrorCategory, INFRASTRUCTURE_ERRORS
from app.telemetry.metrics import REDIS_ERRORS_TOTAL, REDIS_OPERATION_LATENCY_SECONDS


class RedisTelemetryStore:
    LATENCY_BOUNDS_MS = [100, 250, 500, 800, 1200, 2000, 3500, 5000, 10000, 30000]

    def __init__(
        self,
        redis_client: redis.Redis,
        prefix: str = "llmgw",
        window_seconds: int = 60,
        bucket_seconds: int = 5,
    ) -> None:
        self.redis = redis_client
        self.prefix = prefix
        self.window_seconds = window_seconds
        self.bucket_seconds = bucket_seconds

    def _bucket_id(self, now: float | None = None) -> int:
        ts = now if now is not None else time.time()
        return int(ts // self.bucket_seconds)

    def _bucket_key(self, provider: str, bucket_id: int) -> str:
        return f"{self.prefix}:telemetry:{provider}:b:{bucket_id}"

    async def record_outcome(
        self,
        provider: str,
        success: bool,
        latency_ms: float,
        error_category: ErrorCategory | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        cost_usd: float = 0.0,
        retried: bool = False,
        failed_over: bool = False,
        now: float | None = None,
    ) -> None:
        start = time.perf_counter()
        bucket_id = self._bucket_id(now)
        key = self._bucket_key(provider, bucket_id)
        ttl = self.window_seconds + (self.bucket_seconds * 3)

        try:
            pipe = self.redis.pipeline(transaction=False)
            pipe.hincrby(key, "total_requests", 1)
            if success:
                pipe.hincrby(key, "successful_requests", 1)
            else:
                pipe.hincrby(key, "failed_requests", 1)
                if error_category is not None:
                    pipe.hincrby(key, f"err:{error_category.value}", 1)
                    if error_category in INFRASTRUCTURE_ERRORS:
                        pipe.hincrby(key, "infra_errors", 1)

            if retried:
                pipe.hincrby(key, "retries", 1)
            if failed_over:
                pipe.hincrby(key, "failovers", 1)

            pipe.hincrby(key, "prompt_tokens", prompt_tokens)
            pipe.hincrby(key, "completion_tokens", completion_tokens)
            pipe.hincrbyfloat(key, "cost_usd", cost_usd)

            placed = False
            for bound in self.LATENCY_BOUNDS_MS:
                if latency_ms <= bound:
                    pipe.hincrby(key, f"lat_le_{bound}", 1)
                    placed = True
                    break
            if not placed:
                pipe.hincrby(key, "lat_le_inf", 1)

            pipe.expire(key, ttl)
            await pipe.execute()
        except Exception:
            REDIS_ERRORS_TOTAL.labels(operation="record_outcome").inc()
            raise
        finally:
            REDIS_OPERATION_LATENCY_SECONDS.labels(operation="record_outcome").observe(
                time.perf_counter() - start
            )

    async def get_window_stats(self, provider: str, now: float | None = None) -> dict[str, Any]:
        start = time.perf_counter()
        current_bucket = self._bucket_id(now)
        num_buckets = max(1, math.ceil(self.window_seconds / self.bucket_seconds))
        keys = [self._bucket_key(provider, current_bucket - i) for i in range(num_buckets)]

        try:
            pipe = self.redis.pipeline(transaction=False)
            for k in keys:
                pipe.hgetall(k)
            results = await pipe.execute()
        except Exception:
            REDIS_ERRORS_TOTAL.labels(operation="get_window_stats").inc()
            raise
        finally:
            REDIS_OPERATION_LATENCY_SECONDS.labels(operation="get_window_stats").observe(
                time.perf_counter() - start
            )

        agg: dict[str, float] = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "infra_errors": 0,
            "retries": 0,
            "failovers": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "cost_usd": 0.0,
        }
        hist_counts = {b: 0 for b in self.LATENCY_BOUNDS_MS}
        hist_inf = 0
        error_breakdown: dict[str, int] = {cat.value: 0 for cat in ErrorCategory}

        for row in results:
            if not row:
                continue
            for k_raw, v_raw in row.items():
                k = k_raw.decode() if isinstance(k_raw, bytes) else k_raw
                v = v_raw.decode() if isinstance(v_raw, bytes) else v_raw
                if k in agg:
                    agg[k] += float(v)
                elif k.startswith("err:"):
                    cat = k.split(":", 1)[1]
                    error_breakdown[cat] = error_breakdown.get(cat, 0) + int(float(v))
                elif k == "lat_le_inf":
                    hist_inf += int(float(v))
                elif k.startswith("lat_le_"):
                    bound = int(k.split("_")[-1])
                    if bound in hist_counts:
                        hist_counts[bound] += int(float(v))

        total = int(agg["total_requests"])
        infra_errors = int(agg["infra_errors"])
        non_infra_client = error_breakdown["content_filter"] + error_breakdown["client_error"]
        infra_sample_size = max(0, total - non_infra_client)

        success_rate = (
            (infra_sample_size - infra_errors) / infra_sample_size
            if infra_sample_size > 0
            else 1.0
        )
        infra_error_rate = (
            infra_errors / infra_sample_size if infra_sample_size > 0 else 0.0
        )

        p50 = self._estimate_quantile(hist_counts, hist_inf, 0.50)
        p90 = self._estimate_quantile(hist_counts, hist_inf, 0.90)
        p95 = self._estimate_quantile(hist_counts, hist_inf, 0.95)
        p99 = self._estimate_quantile(hist_counts, hist_inf, 0.99)

        return {
            "provider": provider,
            "window_seconds": self.window_seconds,
            "total_requests": total,
            "successful_requests": int(agg["successful_requests"]),
            "failed_requests": int(agg["failed_requests"]),
            "infra_sample_size": infra_sample_size,
            "infra_errors": infra_errors,
            "success_rate": round(success_rate, 4),
            "error_rate": round(infra_error_rate, 4),
            "p50_latency_ms": p50,
            "p90_latency_ms": p90,
            "p95_latency_ms": p95,
            "p99_latency_ms": p99,
            "prompt_tokens": int(agg["prompt_tokens"]),
            "completion_tokens": int(agg["completion_tokens"]),
            "estimated_cost_usd": round(agg["cost_usd"], 6),
            "retries": int(agg["retries"]),
            "failovers": int(agg["failovers"]),
            "errors_by_category": error_breakdown,
        }

    def _estimate_quantile(
        self, hist_counts: dict[int, int], hist_inf: int, q: float
    ) -> float:
        total = sum(hist_counts.values()) + hist_inf
        if total == 0:
            return 0.0
        target = math.ceil(total * q)
        cumulative = 0
        for bound in self.LATENCY_BOUNDS_MS:
            cumulative += hist_counts[bound]
            if cumulative >= target:
                return float(bound)
        return float(self.LATENCY_BOUNDS_MS[-1])
