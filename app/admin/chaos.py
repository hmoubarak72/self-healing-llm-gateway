from __future__ import annotations

import asyncio
import json
import random
import time
import redis.asyncio as redis

from app.core.exceptions import GatewayError
from app.core.models import ChaosRule, ErrorCategory


class ChaosManager:
    def __init__(self, redis_client: redis.Redis, prefix: str = "llmgw") -> None:
        self.redis = redis_client
        self.prefix = prefix

    def _key(self, provider: str) -> str:
        return f"{self.prefix}:chaos:{provider}"

    async def set_rule(self, rule: ChaosRule) -> dict:
        expires_at = time.time() + rule.duration_seconds
        payload = rule.model_dump()
        payload["expires_at"] = expires_at
        await self.redis.set(self._key(rule.provider), json.dumps(payload), ex=rule.duration_seconds)
        return payload

    async def get_rule(self, provider: str) -> dict | None:
        raw = await self.redis.get(self._key(provider))
        if not raw:
            return None
        data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        if data.get("expires_at", 0) <= time.time():
            await self.redis.delete(self._key(provider))
            return None
        return data

    async def list_rules(self, providers: list[str]) -> list[dict]:
        active = []
        for p in providers:
            rule = await self.get_rule(p)
            if rule:
                active.append(rule)
        return active

    async def clear_rules(self, provider: str | None = None, providers: list[str] | None = None) -> int:
        if provider:
            return int(await self.redis.delete(self._key(provider)))
        targets = providers or ["openai", "anthropic", "gemini"]
        deleted = 0
        for p in targets:
            deleted += int(await self.redis.delete(self._key(p)))
        return deleted

    async def maybe_inject(self, provider: str) -> None:
        rule = await self.get_rule(provider)
        if not rule:
            return

        latency_ms = int(rule.get("latency_ms", 0))
        if latency_ms > 0:
            await asyncio.sleep(latency_ms / 1000.0)

        if rule.get("timeout"):
            raise GatewayError(
                f"[Chaos] Simulated timeout on provider '{provider}'",
                category=ErrorCategory.TIMEOUT,
                status_code=504,
                provider=provider,
            )

        error_rate = float(rule.get("error_rate", 0.0))
        if error_rate > 0 and random.random() < error_rate:
            status = int(rule.get("status_code", 500))
            if status == 429:
                raise GatewayError(
                    f"[Chaos] Simulated 429 rate limit on provider '{provider}'",
                    category=ErrorCategory.RATE_LIMIT,
                    status_code=429,
                    provider=provider,
                )
            if status in (401, 403):
                raise GatewayError(
                    f"[Chaos] Simulated auth failure on provider '{provider}'",
                    category=ErrorCategory.AUTH_FAILURE,
                    status_code=status,
                    provider=provider,
                )
            raise GatewayError(
                f"[Chaos] Simulated HTTP {status} failure on provider '{provider}'",
                category=ErrorCategory.SERVER_ERROR,
                status_code=status,
                provider=provider,
            )
