from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
import litellm

from app.admin.chaos import ChaosManager
from app.config import Settings
from app.core.exceptions import GatewayError, classify_exception
from app.core.models import (
    ChatChoice,
    ChatChoiceMessage,
    ChatCompletionRequest,
    ChatCompletionResponse,
    ErrorCategory,
    UsageInfo,
)
from app.providers.base import BaseLLMProviderAdapter

litellm.suppress_debug_info = True


class LiteLLMProviderAdapter(BaseLLMProviderAdapter):
    def __init__(self, settings: Settings, chaos_manager: ChaosManager) -> None:
        self.settings = settings
        self.chaos = chaos_manager

    def _estimate_tokens(self, text: str) -> int:
        return max(1, len(text) // 4)

    async def complete(
        self,
        provider: str,
        model: str,
        request: ChatCompletionRequest,
        timeout_seconds: float,
    ) -> ChatCompletionResponse:
        await self.chaos.maybe_inject(provider)

        joined_prompt = " ".join(
            m.content if isinstance(m.content, str) else json.dumps(m.content)
            for m in request.messages
        )
        if "[TRIGGER_CONTENT_FILTER]" in joined_prompt:
            raise GatewayError(
                "Prompt rejected by provider safety/content_filter policy.",
                category=ErrorCategory.CONTENT_FILTER,
                status_code=400,
                provider=provider,
            )
        if "[TRIGGER_CLIENT_ERROR]" in joined_prompt:
            raise GatewayError(
                "Invalid parameter supplied in completion request.",
                category=ErrorCategory.CLIENT_ERROR,
                status_code=400,
                provider=provider,
            )

        if self.settings.mock_providers_enabled:
            base_delay = {"openai": 0.055, "gemini": 0.045, "anthropic": 0.085}.get(provider, 0.06)
            await asyncio.sleep(base_delay)
            prompt_tokens = self._estimate_tokens(joined_prompt)
            reply = f"[{provider}:{model}] Processed: {joined_prompt[:120]}"
            completion_tokens = self._estimate_tokens(reply)
            return ChatCompletionResponse(
                id=f"chatcmpl-{uuid.uuid4().hex[:16]}",
                created=int(time.time()),
                model=model,
                choices=[
                    ChatChoice(
                        index=0,
                        message=ChatChoiceMessage(role="assistant", content=reply),
                        finish_reason="stop",
                    )
                ],
                usage=UsageInfo(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=prompt_tokens + completion_tokens,
                ),
            )

        provider_cfg = self.settings.providers[provider]
        litellm_model = f"{provider_cfg.litellm_prefix}/{model}"
        messages = [m.model_dump(exclude_none=True) for m in request.messages]

        try:
            resp = await asyncio.wait_for(
                litellm.acompletion(
                    model=litellm_model,
                    messages=messages,
                    temperature=request.temperature,
                    max_tokens=request.max_tokens,
                    top_p=request.top_p,
                    stop=request.stop,
                ),
                timeout=timeout_seconds,
            )
            choice = resp.choices[0]
            usage = getattr(resp, "usage", None)
            prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
            completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
            return ChatCompletionResponse(
                id=getattr(resp, "id", f"chatcmpl-{uuid.uuid4().hex[:16]}"),
                created=int(getattr(resp, "created", time.time())),
                model=model,
                choices=[
                    ChatChoice(
                        index=0,
                        message=ChatChoiceMessage(
                            role="assistant",
                            content=choice.message.content or "",
                        ),
                        finish_reason=getattr(choice, "finish_reason", "stop") or "stop",
                    )
                ],
                usage=UsageInfo(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=prompt_tokens + completion_tokens,
                ),
            )
        except Exception as exc:
            raise classify_exception(exc, provider=provider) from exc

    async def stream(
        self,
        provider: str,
        model: str,
        request: ChatCompletionRequest,
        timeout_seconds: float,
    ) -> AsyncIterator[str]:
        completion = await self.complete(provider, model, request, timeout_seconds)
        content = completion.choices[0].message.content
        words = content.split(" ")
        chunk_id = completion.id
        created = completion.created

        for idx, word in enumerate(words):
            delta_text = word if idx == 0 else f" {word}"
            chunk = {
                "id": chunk_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {"content": delta_text}, "finish_reason": None}],
            }
            yield f"data: {json.dumps(chunk)}

"
            await asyncio.sleep(0.01)

        final_chunk = {
            "id": chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        yield f"data: {json.dumps(final_chunk)}

"
        yield "data: [DONE]

"
