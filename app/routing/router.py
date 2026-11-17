from __future__ import annotations

import asyncio
import time

from app.config import Settings
from app.core.exceptions import GatewayError, UnsupportedModelError, classify_exception
from app.core.models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    ErrorCategory,
    GatewayResponseMetadata,
    INFRASTRUCTURE_ERRORS,
    ResolvedAttribution,
)
from app.cost.calculator import CostCalculator
from app.providers.base import BaseLLMProviderAdapter
from app.resilience.circuit_breaker import DistributedCircuitBreaker
from app.telemetry.metrics import (
    ERRORS_TOTAL,
    ESTIMATED_COST_USD_TOTAL,
    FAILOVERS_TOTAL,
    HEDGED_DUPLICATE_TOKENS_TOTAL,
    HEDGED_REQUESTS_TOTAL,
    REQUESTS_TOTAL,
    REQUEST_LATENCY_SECONDS,
    TOKENS_TOTAL,
)
from app.telemetry.store import RedisTelemetryStore


class GatewayRouter:
    def __init__(
        self,
        settings: Settings,
        adapter: BaseLLMProviderAdapter,
        circuit_breaker: DistributedCircuitBreaker,
        telemetry: RedisTelemetryStore,
        cost_calculator: CostCalculator,
    ) -> None:
        self.settings = settings
        self.adapter = adapter
        self.cb = circuit_breaker
        self.telemetry = telemetry
        self.cost = cost_calculator

    def resolve_target_model(self, requested_model: str, provider: str) -> str:
        aliases = self.settings.model_aliases
        if requested_model in aliases and provider in aliases[requested_model]:
            return aliases[requested_model][provider]
        if requested_model in self.settings.model_pricing:
            return requested_model
        raise UnsupportedModelError(requested_model)

    async def _execute_single_provider(
        self,
        provider: str,
        model: str,
        request: ChatCompletionRequest,
        attribution: ResolvedAttribution,
        is_canary: bool,
        retry_count: int,
        failover_count: int,
    ) -> ChatCompletionResponse:
        provider_cfg = self.settings.providers[provider]
        start = time.perf_counter()
        try:
            resp = await self.adapter.complete(
                provider=provider,
                model=model,
                request=request,
                timeout_seconds=provider_cfg.timeout_seconds,
            )
            latency_ms = (time.perf_counter() - start) * 1000.0
            cost_usd = self.cost.calculate_cost(
                model=model,
                prompt_tokens=resp.usage.prompt_tokens,
                completion_tokens=resp.usage.completion_tokens,
            )

            await self.telemetry.record_outcome(
                provider=provider,
                success=True,
                latency_ms=latency_ms,
                prompt_tokens=resp.usage.prompt_tokens,
                completion_tokens=resp.usage.completion_tokens,
                cost_usd=cost_usd,
                retried=retry_count > 0,
                failed_over=failover_count > 0,
            )
            new_state = await self.cb.record_result(
                provider=provider, success=True, is_canary=is_canary
            )

            REQUEST_LATENCY_SECONDS.labels(
                provider=provider, model=model, request_class=attribution.request_class
            ).observe(latency_ms / 1000.0)
            TOKENS_TOTAL.labels(
                tenant_id=attribution.tenant_id,
                feature_name=attribution.feature_name,
                provider=provider,
                model=model,
                token_type="prompt",
            ).inc(resp.usage.prompt_tokens)
            TOKENS_TOTAL.labels(
                tenant_id=attribution.tenant_id,
                feature_name=attribution.feature_name,
                provider=provider,
                model=model,
                token_type="completion",
            ).inc(resp.usage.completion_tokens)
            ESTIMATED_COST_USD_TOTAL.labels(
                tenant_id=attribution.tenant_id,
                feature_name=attribution.feature_name,
                provider=provider,
                model=model,
            ).inc(cost_usd)

            resp.gateway_metadata = GatewayResponseMetadata(
                provider=provider,
                routed_model=model,
                request_id=attribution.request_id,
                tenant_id=attribution.tenant_id,
                feature_name=attribution.feature_name,
                request_class=attribution.request_class,
                workload_type=attribution.workload_type,
                estimated_cost_usd=cost_usd,
                latency_ms=round(latency_ms, 2),
                retry_count=retry_count,
                failover_count=failover_count,
                hedged=False,
                circuit_state=new_state.value,
            )
            return resp
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            gw_err = classify_exception(exc, provider=provider)
            ERRORS_TOTAL.labels(provider=provider, error_type=gw_err.category.value).inc()
            REQUEST_LATENCY_SECONDS.labels(
                provider=provider, model=model, request_class=attribution.request_class
            ).observe(latency_ms / 1000.0)

            await self.telemetry.record_outcome(
                provider=provider,
                success=False,
                latency_ms=latency_ms,
                error_category=gw_err.category,
                retried=retry_count > 0,
                failed_over=failover_count > 0,
            )
            await self.cb.record_result(
                provider=provider,
                success=False,
                error_category=gw_err.category,
                is_canary=is_canary,
            )
            raise gw_err

    async def route_completion(
        self,
        request: ChatCompletionRequest,
        attribution: ResolvedAttribution,
    ) -> ChatCompletionResponse:
        class_cfg = self.settings.request_classes[attribution.request_class]
        eligible: list[tuple[str, bool]] = []

        for provider in class_cfg.providers:
            if provider not in self.settings.providers or not self.settings.providers[provider].enabled:
                continue
            self.resolve_target_model(request.model, provider)
            allowed, is_canary, _ = await self.cb.evaluate_admission(provider)
            if allowed:
                eligible.append((provider, is_canary))

        if not eligible:
            REQUESTS_TOTAL.labels(
                provider="none",
                model=request.model,
                tenant_id=attribution.tenant_id,
                feature_name=attribution.feature_name,
                request_class=attribution.request_class,
                workload_type=attribution.workload_type,
                outcome="all_circuits_open",
            ).inc()
            raise GatewayError(
                "All eligible providers are currently circuit-broken (OPEN).",
                category=ErrorCategory.SERVER_ERROR,
                status_code=503,
            )

        if class_cfg.hedge.enabled and len(eligible) >= 2 and not eligible[0][1]:
            return await self._route_with_hedging(
                request=request,
                attribution=attribution,
                eligible=eligible,
                threshold_ms=class_cfg.hedge.threshold_ms,
            )

        return await self._route_sequential_failover(
            request=request,
            attribution=attribution,
            eligible=eligible,
        )

    async def _route_sequential_failover(
        self,
        request: ChatCompletionRequest,
        attribution: ResolvedAttribution,
        eligible: list[tuple[str, bool]],
    ) -> ChatCompletionResponse:
        last_error: GatewayError | None = None
        failover_count = 0

        for idx, (provider, is_canary) in enumerate(eligible):
            target_model = self.resolve_target_model(request.model, provider)
            try:
                resp = await self._execute_single_provider(
                    provider=provider,
                    model=target_model,
                    request=request,
                    attribution=attribution,
                    is_canary=is_canary,
                    retry_count=idx,
                    failover_count=failover_count,
                )
                REQUESTS_TOTAL.labels(
                    provider=provider,
                    model=target_model,
                    tenant_id=attribution.tenant_id,
                    feature_name=attribution.feature_name,
                    request_class=attribution.request_class,
                    workload_type=attribution.workload_type,
                    outcome="success",
                ).inc()
                return resp
            except GatewayError as err:
                last_error = err
                if err.category not in INFRASTRUCTURE_ERRORS:
                    REQUESTS_TOTAL.labels(
                        provider=provider,
                        model=target_model,
                        tenant_id=attribution.tenant_id,
                        feature_name=attribution.feature_name,
                        request_class=attribution.request_class,
                        workload_type=attribution.workload_type,
                        outcome=err.category.value,
                    ).inc()
                    raise err

                if idx + 1 < len(eligible):
                    next_provider = eligible[idx + 1][0]
                    failover_count += 1
                    FAILOVERS_TOTAL.labels(
                        from_provider=provider,
                        to_provider=next_provider,
                        request_class=attribution.request_class,
                    ).inc()

        REQUESTS_TOTAL.labels(
            provider=eligible[-1][0],
            model=request.model,
            tenant_id=attribution.tenant_id,
            feature_name=attribution.feature_name,
            request_class=attribution.request_class,
            workload_type=attribution.workload_type,
            outcome="exhausted",
        ).inc()
        assert last_error is not None
        raise last_error

    async def _route_with_hedging(
        self,
        request: ChatCompletionRequest,
        attribution: ResolvedAttribution,
        eligible: list[tuple[str, bool]],
        threshold_ms: int,
    ) -> ChatCompletionResponse:
        primary_provider, primary_canary = eligible[0]
        secondary_provider, secondary_canary = eligible[1]
        primary_model = self.resolve_target_model(request.model, primary_provider)
        secondary_model = self.resolve_target_model(request.model, secondary_provider)

        primary_task = asyncio.create_task(
            self._execute_single_provider(
                provider=primary_provider,
                model=primary_model,
                request=request,
                attribution=attribution,
                is_canary=primary_canary,
                retry_count=0,
                failover_count=0,
            )
        )

        done, _ = await asyncio.wait({primary_task}, timeout=threshold_ms / 1000.0)
        if primary_task in done:
            try:
                resp = primary_task.result()
                REQUESTS_TOTAL.labels(
                    provider=primary_provider,
                    model=primary_model,
                    tenant_id=attribution.tenant_id,
                    feature_name=attribution.feature_name,
                    request_class=attribution.request_class,
                    workload_type=attribution.workload_type,
                    outcome="success",
                ).inc()
                return resp
            except GatewayError as err:
                if err.category not in INFRASTRUCTURE_ERRORS:
                    raise err
                FAILOVERS_TOTAL.labels(
                    from_provider=primary_provider,
                    to_provider=secondary_provider,
                    request_class=attribution.request_class,
                ).inc()
                return await self._route_sequential_failover(
                    request=request,
                    attribution=attribution,
                    eligible=eligible[1:],
                )

        secondary_task = asyncio.create_task(
            self._execute_single_provider(
                provider=secondary_provider,
                model=secondary_model,
                request=request,
                attribution=attribution,
                is_canary=secondary_canary,
                retry_count=1,
                failover_count=1,
            )
        )

        tasks = {primary_task: primary_provider, secondary_task: secondary_provider}
        pending = set(tasks.keys())
        winner_resp: ChatCompletionResponse | None = None
        winner_provider: str | None = None
        errors: list[GatewayError] = []

        while pending and winner_resp is None:
            finished, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for t in finished:
                try:
                    winner_resp = t.result()
                    winner_provider = tasks[t]
                except GatewayError as err:
                    errors.append(err)

        for loser in pending:
            loser.cancel()
            try:
                await loser
            except (asyncio.CancelledError, Exception):
                pass

        if winner_resp is not None and winner_provider is not None:
            HEDGED_REQUESTS_TOTAL.labels(
                primary_provider=primary_provider,
                secondary_provider=secondary_provider,
                winner_provider=winner_provider,
                request_class=attribution.request_class,
            ).inc()
            if winner_resp.gateway_metadata:
                winner_resp.gateway_metadata.hedged = True
            for t, prov in tasks.items():
                if prov != winner_provider and t.done() and not t.cancelled() and t.exception() is None:
                    loser_res = t.result()
                    HEDGED_DUPLICATE_TOKENS_TOTAL.labels(
                        provider=prov, request_class=attribution.request_class
                    ).inc(loser_res.usage.total_tokens)

            REQUESTS_TOTAL.labels(
                provider=winner_provider,
                model=winner_resp.model,
                tenant_id=attribution.tenant_id,
                feature_name=attribution.feature_name,
                request_class=attribution.request_class,
                workload_type=attribution.workload_type,
                outcome="hedged_success",
            ).inc()
            return winner_resp

        if len(eligible) > 2:
            return await self._route_sequential_failover(
                request=request,
                attribution=attribution,
                eligible=eligible[2:],
            )
        raise errors[-1]
