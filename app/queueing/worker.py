from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any
import redis.asyncio as redis

from app.config import Settings
from app.core.exceptions import GatewayError
from app.core.models import (
    ChatCompletionRequest,
    QueuedCompletionResponse,
    RETRYABLE_ERRORS,
    ResolvedAttribution,
)
from app.queueing.idempotency import IdempotencyManager
from app.routing.router import GatewayRouter
from app.telemetry.metrics import (
    QUEUE_DEAD_LETTERS_TOTAL,
    QUEUE_DEPTH,
    QUEUE_PROCESSING_LATENCY_SECONDS,
    QUEUE_RETRIES_TOTAL,
)

logger = logging.getLogger(__name__)


class DeferredQueueManager:
    def __init__(
        self,
        redis_client: redis.Redis,
        router: GatewayRouter,
        idempotency: IdempotencyManager,
        settings: Settings,
    ) -> None:
        self.redis = redis_client
        self.router = router
        self.idempotency = idempotency
        self.settings = settings
        self.prefix = settings.redis_key_prefix
        self._running = False
        self._task: asyncio.Task | None = None

    @property
    def schedule_key(self) -> str:
        return f"{self.prefix}:queue:schedule"

    @property
    def dlq_key(self) -> str:
        return f"{self.prefix}:queue:dlq"

    def _job_key(self, request_id: str) -> str:
        return f"{self.prefix}:queue:job:{request_id}"

    async def queue_depth(self) -> int:
        depth = int(await self.redis.zcard(self.schedule_key))
        QUEUE_DEPTH.set(depth)
        return depth

    async def enqueue(
        self,
        request: ChatCompletionRequest,
        attribution: ResolvedAttribution,
    ) -> QueuedCompletionResponse:
        idem_key = attribution.idempotency_key or f"auto-{attribution.request_id}"
        now = time.time()
        job_record = {
            "request_id": attribution.request_id,
            "idempotency_key": idem_key,
            "tenant_id": attribution.tenant_id,
            "feature_name": attribution.feature_name,
            "request_class": attribution.request_class,
            "status": "QUEUED",
            "attempt": 0,
            "enqueued_at": now,
            "deadline_at": now + self.settings.queue_deadline_seconds,
            "request_payload": request.model_dump(),
            "attribution": attribution.model_dump(),
            "last_error": None,
            "response": None,
        }
        await self.redis.set(
            self._job_key(attribution.request_id),
            json.dumps(job_record),
            ex=self.settings.idempotency_ttl_seconds,
        )
        delay = self._compute_backoff(attempt=0)
        await self.redis.zadd(self.schedule_key, {attribution.request_id: now + delay})
        depth = await self.queue_depth()

        queued_resp = QueuedCompletionResponse(
            request_id=attribution.request_id,
            idempotency_key=idem_key,
            tenant_id=attribution.tenant_id,
            feature_name=attribution.feature_name,
            request_class=attribution.request_class,
            queue_position=depth,
            status_url=f"/v1/requests/{attribution.request_id}",
        )
        await self.idempotency.mark_queued(
            tenant_id=attribution.tenant_id,
            idempotency_key=idem_key,
            queue_payload=queued_resp.model_dump(),
        )
        return queued_resp

    def _compute_backoff(self, attempt: int) -> float:
        exp = min(
            self.settings.queue_max_backoff_seconds,
            self.settings.queue_base_backoff_seconds * (2**attempt),
        )
        return round(exp * (0.5 + 0.5 * random.random()), 3)

    async def get_job_status(self, request_id: str) -> dict[str, Any] | None:
        raw = await self.redis.get(self._job_key(request_id))
        if not raw:
            return None
        data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        return {
            "request_id": data["request_id"],
            "idempotency_key": data["idempotency_key"],
            "tenant_id": data["tenant_id"],
            "feature_name": data["feature_name"],
            "request_class": data["request_class"],
            "status": data["status"],
            "attempt": data["attempt"],
            "enqueued_at": data["enqueued_at"],
            "last_error": data["last_error"],
            "response": data["response"],
        }

    async def process_due_jobs_once(self) -> int:
        now = time.time()
        due_ids = await self.redis.zrangebyscore(self.schedule_key, min=0, max=now, start=0, num=10)
        processed = 0
        for raw_id in due_ids:
            req_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            removed = await self.redis.zrem(self.schedule_key, req_id)
            if not removed:
                continue
            await self._process_single_job(req_id)
            processed += 1
        await self.queue_depth()
        return processed

    async def _process_single_job(self, request_id: str) -> None:
        raw = await self.redis.get(self._job_key(request_id))
        if not raw:
            return
        job = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        req_class = job["request_class"]

        if time.time() > job["deadline_at"]:
            job["status"] = "DEAD_LETTER"
            job["last_error"] = "Queue deadline exceeded."
            await self.redis.set(self._job_key(request_id), json.dumps(job), ex=self.settings.idempotency_ttl_seconds)
            await self.redis.lpush(self.dlq_key, json.dumps(job))
            QUEUE_DEAD_LETTERS_TOTAL.labels(request_class=req_class, reason="deadline_exceeded").inc()
            return

        req_obj = ChatCompletionRequest(**job["request_payload"])
        attr_obj = ResolvedAttribution(**job["attribution"])

        try:
            resp = await self.router.route_completion(req_obj, attr_obj)
            resp_dict = resp.model_dump()
            job["status"] = "COMPLETED"
            job["response"] = resp_dict
            await self.redis.set(
                self._job_key(request_id),
                json.dumps(job),
                ex=self.settings.idempotency_ttl_seconds,
            )
            await self.idempotency.mark_completed(
                tenant_id=attr_obj.tenant_id,
                idempotency_key=job["idempotency_key"],
                response_payload=resp_dict,
            )
            QUEUE_PROCESSING_LATENCY_SECONDS.labels(request_class=req_class).observe(
                max(0.0, time.time() - job["enqueued_at"])
            )
        except GatewayError as err:
            attempt = int(job["attempt"]) + 1
            job["attempt"] = attempt
            job["last_error"] = err.message
            QUEUE_RETRIES_TOTAL.labels(request_class=req_class).inc()

            if err.category not in RETRYABLE_ERRORS or attempt >= self.settings.queue_max_retries:
                job["status"] = "DEAD_LETTER"
                await self.redis.set(
                    self._job_key(request_id),
                    json.dumps(job),
                    ex=self.settings.idempotency_ttl_seconds,
                )
                await self.redis.lpush(self.dlq_key, json.dumps(job))
                reason = "non_retryable" if err.category not in RETRYABLE_ERRORS else "max_retries"
                QUEUE_DEAD_LETTERS_TOTAL.labels(request_class=req_class, reason=reason).inc()
                return

            delay = self._compute_backoff(attempt)
            await self.redis.set(
                self._job_key(request_id),
                json.dumps(job),
                ex=self.settings.idempotency_ttl_seconds,
            )
            await self.redis.zadd(self.schedule_key, {request_id: time.time() + delay})

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.process_due_jobs_once()
            except Exception as exc:
                logger.exception("Deferred queue worker error: %s", exc)
            await asyncio.sleep(self.settings.queue_poll_interval_seconds)
