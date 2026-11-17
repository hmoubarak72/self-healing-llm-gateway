from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from app.core.models import ChatCompletionRequest, ChatCompletionResponse


class BaseLLMProviderAdapter(ABC):
    @abstractmethod
    async def complete(
        self,
        provider: str,
        model: str,
        request: ChatCompletionRequest,
        timeout_seconds: float,
    ) -> ChatCompletionResponse:
        ...

    @abstractmethod
    def stream(
        self,
        provider: str,
        model: str,
        request: ChatCompletionRequest,
        timeout_seconds: float,
    ) -> AsyncIterator[str]:
        ...
