from __future__ import annotations

import asyncio
import json
from typing import Any
import redis.asyncio as redis


class IdempotencyManager:
    def __init__(
        self,
        redis_client: redis.Redis,
        prefix: str = "llmgw",
        ttl_seconds: int = 3600,
        lock_ttl_seconds: int = 30,
    ) -> None:
        self.redis = redis_client
        self.prefix = prefix
        self.ttl_seconds = ttl_seconds
        self.lock_ttl_seconds = lock_ttl_seconds

    def _key(self, tenant_id: str, idempotency_key: str) -> str:
        return f"{self.prefix}:idem:{tenant_id}:{idempotency_key}"

    async def acquire_or_fetch(
        self, tenant_id: str, idempotency_key: str, request_id: str
    ) -> tuple[str, dict[str, Any] | None]:
        key = self._key(tenant_id, idempotency_key)
        initial = json.dumps({"status": "IN_PROGRESS", "request_id": request_id})
        acquired = await self.redis.set(key, initial, nx=True, ex=self.lock_ttl_seconds)
        if acquired:
            return "ACQUIRED", None

        for _ in range(6):
            raw = await self.redis.get(key)
            if not raw:
                retry_acq = await self.redis.set(key, initial, nx=True, ex=self.lock_ttl_seconds)
                if retry_acq:
                    return "ACQUIRED", None
                break
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            status = data.get("status", "IN_PROGRESS")
            if status in ("COMPLETED", "QUEUED"):
                return status, data
            await asyncio.sleep(0.25)

        raw = await self.redis.get(key)
        if raw:
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            return data.get("status", "IN_PROGRESS"), data
        return "IN_PROGRESS", {"status": "IN_PROGRESS", "request_id": request_id}

    async def mark_completed(
        self, tenant_id: str, idempotency_key: str, response_payload: dict[str, Any]
    ) -> None:
        key = self._key(tenant_id, idempotency_key)
        record = {"status": "COMPLETED", "response": response_payload}
        await self.redis.set(key, json.dumps(record), ex=self.ttl_seconds)

    async def mark_queued(
        self, tenant_id: str, idempotency_key: str, queue_payload: dict[str, Any]
    ) -> None:
        key = self._key(tenant_id, idempotency_key)
        record = {"status": "QUEUED", "queue_info": queue_payload}
        await self.redis.set(key, json.dumps(record), ex=self.ttl_seconds)

    async def release_on_failure(self, tenant_id: str, idempotency_key: str) -> None:
        key = self._key(tenant_id, idempotency_key)
        raw = await self.redis.get(key)
        if raw:
            data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            if data.get("status") == "IN_PROGRESS":
                await self.redis.delete(key)

    async def get_record(self, tenant_id: str, idempotency_key: str) -> dict[str, Any] | None:
        raw = await self.redis.get(self._key(tenant_id, idempotency_key))
        if not raw:
            return None
        return json.loads(raw.decode() if isinstance(raw, bytes) else raw)
