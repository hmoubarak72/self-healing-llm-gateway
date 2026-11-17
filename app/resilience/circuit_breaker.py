from __future__ import annotations

import random
import time
from typing import Any
import redis.asyncio as redis

from app.config import Settings
from app.core.models import (
    CIRCUIT_STATE_NUMERIC,
    CircuitState,
    ErrorCategory,
    INFRASTRUCTURE_ERRORS,
)
from app.telemetry.metrics import CIRCUIT_BREAKER_STATE, CIRCUIT_BREAKER_TRANSITIONS_TOTAL
from app.telemetry.store import RedisTelemetryStore

TRANSITION_LUA = """
local key = KEYS[1]
local expected_from = ARGV[1]
local target_to = ARGV[2]
local now_ts = ARGV[3]

local current = redis.call('HGET', key, 'state')
if not current then
    current = 'CLOSED'
end

if current ~= expected_from then
    return {0, current}
end

redis.call('HSET', key,
    'state', target_to,
    'last_transition_ts', now_ts,
    'consecutive_successes', 0,
    'consecutive_failures', 0
)
return {1, target_to}
"""


class DistributedCircuitBreaker:
    def __init__(
        self,
        redis_client: redis.Redis,
        telemetry: RedisTelemetryStore,
        settings: Settings,
    ) -> None:
        self.redis = redis_client
        self.telemetry = telemetry
        self.settings = settings
        self.prefix = settings.redis_key_prefix
        self._transition_script = self.redis.register_script(TRANSITION_LUA)

    def _key(self, provider: str) -> str:
        return f"{self.prefix}:cb:{provider}"

    async def get_state_info(self, provider: str) -> dict[str, Any]:
        raw = await self.redis.hgetall(self._key(provider))
        decoded = {
            (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in raw.items()
        }
        state = CircuitState(decoded.get("state", CircuitState.CLOSED.value))
        last_ts = float(decoded.get("last_transition_ts", 0.0))
        succ = int(decoded.get("consecutive_successes", 0))
        fail = int(decoded.get("consecutive_failures", 0))

        CIRCUIT_BREAKER_STATE.labels(provider=provider).set(CIRCUIT_STATE_NUMERIC[state])
        return {
            "provider": provider,
            "state": state,
            "last_transition_ts": last_ts,
            "consecutive_successes": succ,
            "consecutive_failures": fail,
        }

    async def _atomic_transition(
        self, provider: str, expected_from: CircuitState, target_to: CircuitState
    ) -> bool:
        now_ts = str(time.time())
        res = await self._transition_script(
            keys=[self._key(provider)],
            args=[expected_from.value, target_to.value, now_ts],
        )
        changed = int(res[0]) == 1
        if changed:
            CIRCUIT_BREAKER_STATE.labels(provider=provider).set(CIRCUIT_STATE_NUMERIC[target_to])
            CIRCUIT_BREAKER_TRANSITIONS_TOTAL.labels(
                provider=provider,
                from_state=expected_from.value,
                to_state=target_to.value,
            ).inc()
        return changed

    async def evaluate_admission(self, provider: str) -> tuple[bool, bool, CircuitState]:
        info = await self.get_state_info(provider)
        state: CircuitState = info["state"]

        if state == CircuitState.CLOSED:
            return True, False, CircuitState.CLOSED

        if state == CircuitState.OPEN:
            elapsed = time.time() - info["last_transition_ts"]
            if elapsed >= self.settings.cb_open_duration_seconds:
                await self._atomic_transition(provider, CircuitState.OPEN, CircuitState.HALF_OPEN)
                return True, True, CircuitState.HALF_OPEN
            return False, False, CircuitState.OPEN

        if random.random() < self.settings.cb_half_open_canary_rate:
            return True, True, CircuitState.HALF_OPEN
        return False, False, CircuitState.HALF_OPEN

    async def record_result(
        self,
        provider: str,
        success: bool,
        error_category: ErrorCategory | None = None,
        is_canary: bool = False,
    ) -> CircuitState:
        info = await self.get_state_info(provider)
        state: CircuitState = info["state"]
        key = self._key(provider)

        if not success and error_category not in INFRASTRUCTURE_ERRORS:
            return state

        if state == CircuitState.HALF_OPEN:
            if success:
                await self.redis.hset(key, "consecutive_failures", 0)
                succ = int(await self.redis.hincrby(key, "consecutive_successes", 1))
                if succ >= self.settings.cb_required_successes_to_close:
                    await self._atomic_transition(
                        provider, CircuitState.HALF_OPEN, CircuitState.CLOSED
                    )
                    return CircuitState.CLOSED
                return CircuitState.HALF_OPEN
            else:
                await self.redis.hset(key, "consecutive_successes", 0)
                fails = int(await self.redis.hincrby(key, "consecutive_failures", 1))
                if fails >= self.settings.cb_required_failures_to_reopen:
                    await self._atomic_transition(
                        provider, CircuitState.HALF_OPEN, CircuitState.OPEN
                    )
                    return CircuitState.OPEN
                return CircuitState.HALF_OPEN

        if state == CircuitState.CLOSED:
            stats = await self.telemetry.get_window_stats(provider)
            sample_size = stats["infra_sample_size"]
            if sample_size >= self.settings.cb_min_sample_size:
                error_rate_tripped = stats["error_rate"] >= self.settings.cb_error_rate_threshold
                latency_tripped = (
                    stats["p95_latency_ms"] >= self.settings.cb_p95_latency_threshold_ms
                )
                if error_rate_tripped or latency_tripped:
                    await self._atomic_transition(
                        provider, CircuitState.CLOSED, CircuitState.OPEN
                    )
                    return CircuitState.OPEN

        return (await self.get_state_info(provider))["state"]

    async def force_state(self, provider: str, target_state: CircuitState) -> dict[str, Any]:
        info = await self.get_state_info(provider)
        current = info["state"]
        if current != target_state:
            await self._atomic_transition(provider, current, target_state)
        return await self.get_state_info(provider)
