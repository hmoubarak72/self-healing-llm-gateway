from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.admin.chaos import ChaosManager
from app.api.dependencies import verify_admin_token
from app.config import Settings
from app.core.exceptions import GatewayError
from app.core.models import (
    ChaosRule,
    ChatCompletionRequest,
    ChatCompletionResponse,
    CircuitState,
    QueuedCompletionResponse,
)
from app.providers.base import BaseLLMProviderAdapter
from app.queueing.idempotency import IdempotencyManager
from app.queueing.worker import DeferredQueueManager
from app.resilience.circuit_breaker import DistributedCircuitBreaker
from app.routing.router import GatewayRouter
from app.telemetry.store import RedisTelemetryStore
from app.utils.attribution import resolve_attribution

router = APIRouter()


@router.get("/health")
async def health_check(request: Request) -> dict[str, Any]:
    redis_client = request.app.state.redis
    try:
        pong = await redis_client.ping()
        redis_ok = bool(pong)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis unhealthy: {exc}") from exc
    return {"status": "ok", "redis": redis_ok}


@router.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.post("/v1/chat/completions", response_model=None)
async def chat_completions(
    payload: ChatCompletionRequest,
    request: Request,
) -> ChatCompletionResponse | QueuedCompletionResponse | StreamingResponse | JSONResponse:
    settings: Settings = request.app.state.settings
    gw_router: GatewayRouter = request.app.state.router
    idem: IdempotencyManager = request.app.state.idempotency
    queue_mgr: DeferredQueueManager = request.app.state.queue_manager
    adapter: BaseLLMProviderAdapter = request.app.state.adapter

    attribution = resolve_attribution(request, payload, settings)

    if attribution.idempotency_key:
        status, record = await idem.acquire_or_fetch(
            tenant_id=attribution.tenant_id,
            idempotency_key=attribution.idempotency_key,
            request_id=attribution.request_id,
        )
        if status == "COMPLETED" and record and "response" in record:
            return JSONResponse(status_code=200, content=record["response"])
        if status == "QUEUED" and record and "queue_info" in record:
            return JSONResponse(status_code=202, content=record["queue_info"])
        if status == "IN_PROGRESS":
            return JSONResponse(
                status_code=409,
                content={
                    "error": {
                        "message": "A request with this idempotency_key is currently in progress.",
                        "type": "idempotency_conflict",
                        "request_id": record.get("request_id") if record else attribution.request_id,
                    }
                },
            )

    if payload.stream:
        class_cfg = settings.request_classes[attribution.request_class]
        selected_provider = class_cfg.providers[0]
        target_model = gw_router.resolve_target_model(payload.model, selected_provider)
        if attribution.idempotency_key:
            await idem.release_on_failure(attribution.tenant_id, attribution.idempotency_key)
        return StreamingResponse(
            adapter.stream(
                provider=selected_provider,
                model=target_model,
                request=payload,
                timeout_seconds=settings.providers[selected_provider].timeout_seconds,
            ),
            media_type="text/event-stream",
        )

    try:
        resp = await gw_router.route_completion(request=payload, attribution=attribution)
        if attribution.idempotency_key:
            await idem.mark_completed(
                tenant_id=attribution.tenant_id,
                idempotency_key=attribution.idempotency_key,
                response_payload=resp.model_dump(),
            )
        return resp
    except GatewayError as err:
        if attribution.workload_type == "deferrable" and err.status_code >= 500:
            queued = await queue_mgr.enqueue(request=payload, attribution=attribution)
            return JSONResponse(status_code=202, content=queued.model_dump())

        if attribution.idempotency_key:
            await idem.release_on_failure(attribution.tenant_id, attribution.idempotency_key)
        raise err


@router.get("/v1/requests/{request_id}")
async def get_request_status(request_id: str, request: Request) -> dict[str, Any]:
    queue_mgr: DeferredQueueManager = request.app.state.queue_manager
    job = await queue_mgr.get_job_status(request_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"No queued/tracked request '{request_id}' found.")
    return job


@router.post("/admin/chaos", dependencies=[Depends(verify_admin_token)])
async def inject_chaos(rule: ChaosRule, request: Request) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    if rule.provider not in settings.providers:
        raise HTTPException(status_code=400, detail=f"Unknown provider '{rule.provider}'.")
    chaos: ChaosManager = request.app.state.chaos
    created = await chaos.set_rule(rule)
    return {"status": "active", "rule": created}


@router.get("/admin/chaos", dependencies=[Depends(verify_admin_token)])
async def list_chaos(request: Request) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    chaos: ChaosManager = request.app.state.chaos
    rules = await chaos.list_rules(list(settings.providers.keys()))
    return {"active_rules": rules}


@router.delete("/admin/chaos", dependencies=[Depends(verify_admin_token)])
async def clear_chaos(request: Request, provider: str | None = None) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    chaos: ChaosManager = request.app.state.chaos
    deleted = await chaos.clear_rules(
        provider=provider, providers=list(settings.providers.keys())
    )
    return {"cleared_count": deleted}


@router.get("/admin/circuits", dependencies=[Depends(verify_admin_token)])
async def get_circuit_states(request: Request) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    cb: DistributedCircuitBreaker = request.app.state.circuit_breaker
    telemetry: RedisTelemetryStore = request.app.state.telemetry
    states = {}
    for p in settings.providers:
        c_info = await cb.get_state_info(p)
        w_stats = await telemetry.get_window_stats(p)
        states[p] = {**c_info, "state": c_info["state"].value, "rolling_window": w_stats}
    return {"providers": states}


@router.post("/admin/circuits/{provider}/probe", dependencies=[Depends(verify_admin_token)])
async def synthetic_health_probe(provider: str, request: Request) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    if provider not in settings.providers:
        raise HTTPException(status_code=400, detail=f"Unknown provider '{provider}'.")
    cb: DistributedCircuitBreaker = request.app.state.circuit_breaker
    info = await cb.force_state(provider, CircuitState.HALF_OPEN)
    return {"status": "probing", "circuit": {**info, "state": info["state"].value}}


@router.get("/admin/queue", dependencies=[Depends(verify_admin_token)])
async def get_queue_metrics(request: Request) -> dict[str, Any]:
    queue_mgr: DeferredQueueManager = request.app.state.queue_manager
    depth = await queue_mgr.queue_depth()
    dlq_len = int(await request.app.state.redis.llen(queue_mgr.dlq_key))
    return {"queue_depth": depth, "dead_letter_depth": dlq_len}
