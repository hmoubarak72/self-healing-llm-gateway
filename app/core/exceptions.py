from __future__ import annotations

from app.core.models import ErrorCategory


class GatewayError(Exception):
    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        status_code: int = 502,
        provider: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.category = category
        self.status_code = status_code
        self.provider = provider


class MetadataValidationError(GatewayError):
    def __init__(self, message: str) -> None:
        super().__init__(message=message, category=ErrorCategory.CLIENT_ERROR, status_code=400)


class UnsupportedModelError(GatewayError):
    def __init__(self, model: str) -> None:
        super().__init__(
            message=f"Unsupported model or no configured provider mapping for '{model}'.",
            category=ErrorCategory.CLIENT_ERROR,
            status_code=400,
        )


def classify_exception(exc: Exception, provider: str | None = None) -> GatewayError:
    if isinstance(exc, GatewayError):
        return exc

    msg = str(exc).lower()
    status_code = getattr(exc, "status_code", None)

    if status_code == 429 or "rate limit" in msg or "429" in msg:
        return GatewayError(str(exc), ErrorCategory.RATE_LIMIT, 429, provider)
    if isinstance(exc, TimeoutError) or "timeout" in msg or "timed out" in msg or "deadline" in msg:
        return GatewayError(str(exc), ErrorCategory.TIMEOUT, 504, provider)
    if status_code in (401, 403) or "authentication" in msg or "unauthorized" in msg or "api key" in msg:
        return GatewayError(str(exc), ErrorCategory.AUTH_FAILURE, status_code or 401, provider)
    if "content_filter" in msg or "safety" in msg or "moderation" in msg or "content policy" in msg:
        return GatewayError(str(exc), ErrorCategory.CONTENT_FILTER, 400, provider)
    if status_code in (400, 404, 422) or "invalid request" in msg or "bad request" in msg:
        return GatewayError(str(exc), ErrorCategory.CLIENT_ERROR, status_code or 400, provider)
    if (status_code and status_code >= 500) or "service unavailable" in msg or "internal server error" in msg or "502" in msg or "503" in msg:
        return GatewayError(str(exc), ErrorCategory.SERVER_ERROR, status_code or 502, provider)

    return GatewayError(str(exc), ErrorCategory.UNKNOWN, 502, provider)
