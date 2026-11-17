from __future__ import annotations

from contextlib import asynccontextmanager
import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.admin.chaos import ChaosManager
from app.api.routes import router
from app.config import Settings, get_settings
from app.core.exceptions import GatewayError
from app.cost.calculator import CostCalculator
from app.providers.litellm_adapter import LiteLLMProviderAdapter
from app.queueing.idempotency import IdempotencyManager
from app.queueing.worker import DeferredQueueManager
from app.resilience.circuit_breaker import DistributedCircuitBreaker
from app.routing.router import GatewayRouter
from app.telemetry.store import RedisTelemetryStore


def create_app(
    settings: Settings | None = None,
    redis_client: redis.Redis | None = None,
    start_worker: bool = True,
) -> FastAPI:
    cfg = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        r_client = redis_client or redis.from_url(cfg.redis_url, decode_responses=False)
        telemetry = RedisTelemetryStore(
            redis_client=r_client,
            prefix=cfg.redis_key_prefix,
            window_seconds=cfg.telemetry_window_seconds,
            bucket_seconds=cfg.telemetry_bucket_seconds,
        )
        cb = DistributedCircuitBreaker(
            redis_client=r_client,
            telemetry=telemetry,
            settings=cfg,
        )
        chaos = ChaosManager(redis_client=r_client, prefix=cfg.redis_key_prefix)
        adapter = LiteLLMProviderAdapter(settings=cfg, chaos_manager=chaos)
        cost_calc = CostCalculator(pricing_table=cfg.model_pricing)
        gw_router = GatewayRouter(
            settings=cfg,
            adapter=adapter,
            circuit_breaker=cb,
            telemetry=telemetry,
            cost_calculator=cost_calc,
        )
        idempotency = IdempotencyManager(
            redis_client=r_client,
            prefix=cfg.redis_key_prefix,
            ttl_seconds=cfg.idempotency_ttl_seconds,
            lock_ttl_seconds=cfg.idempotency_lock_ttl_seconds,
        )
        queue_mgr = DeferredQueueManager(
            redis_client=r_client,
            router=gw_router,
            idempotency=idempotency,
            settings=cfg,
        )

        app.state.settings = cfg
        app.state.redis = r_client
        app.state.telemetry = telemetry
        app.state.circuit_breaker = cb
        app.state.chaos = chaos
        app.state.adapter = adapter
        app.state.cost_calculator = cost_calc
        app.state.router = gw_router
        app.state.idempotency = idempotency
        app.state.queue_manager = queue_mgr

        if start_worker:
            await queue_mgr.start()
        yield
        if start_worker:
            await queue_mgr.stop()
        if redis_client is None:
            await r_client.aclose()

    app = FastAPI(
        title="Self-Healing LLM Gateway",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "message": exc.message,
                    "type": exc.category.value,
                    "provider": exc.provider,
                }
            },
        )

    app.include_router(router)
    return app


app = create_app()
