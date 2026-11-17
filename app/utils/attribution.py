from __future__ import annotations

from fastapi import Request
from app.config import Settings
from app.core.exceptions import MetadataValidationError
from app.core.models import ChatCompletionRequest, ResolvedAttribution


def resolve_attribution(
    request: Request,
    payload: ChatCompletionRequest,
    settings: Settings,
) -> ResolvedAttribution:
    meta = payload.metadata

    tenant_id = (
        request.headers.get("X-Tenant-ID")
        or payload.tenant_id
        or (meta.tenant_id if meta else None)
    )
    feature_name = (
        request.headers.get("X-Feature-Name")
        or payload.feature_name
        or (meta.feature_name if meta else None)
    )
    request_id = (
        request.headers.get("X-Request-ID")
        or payload.request_id
        or (meta.request_id if meta else None)
    )

    missing = []
    if not tenant_id or not tenant_id.strip():
        missing.append("tenant_id (or X-Tenant-ID)")
    if not feature_name or not feature_name.strip():
        missing.append("feature_name (or X-Feature-Name)")
    if not request_id or not request_id.strip():
        missing.append("request_id (or X-Request-ID)")

    if missing:
        raise MetadataValidationError(
            f"Missing required attribution metadata: {', '.join(missing)}"
        )

    request_class = (
        request.headers.get("X-Request-Class")
        or payload.request_class
        or (meta.request_class if meta else None)
        or "low_latency_classification"
    )
    if request_class not in settings.request_classes:
        raise MetadataValidationError(f"Unknown request_class '{request_class}'.")

    class_cfg = settings.request_classes[request_class]
    raw_workload = (
        request.headers.get("X-Workload-Type")
        or payload.workload_type
        or (meta.workload_type if meta else None)
        or class_cfg.workload_type
    )
    if raw_workload not in ("interactive", "deferrable"):
        raise MetadataValidationError(f"Invalid workload_type '{raw_workload}'.")

    idempotency_key = (
        request.headers.get("Idempotency-Key")
        or request.headers.get("X-Idempotency-Key")
        or payload.idempotency_key
        or (meta.idempotency_key if meta else None)
    )

    return ResolvedAttribution(
        tenant_id=tenant_id.strip(),
        feature_name=feature_name.strip(),
        request_id=request_id.strip(),
        request_class=request_class.strip(),
        workload_type=raw_workload,  # type: ignore[arg-type]
        idempotency_key=idempotency_key.strip() if idempotency_key else None,
    )
