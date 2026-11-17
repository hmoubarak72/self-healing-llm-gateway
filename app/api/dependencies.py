from __future__ import annotations

from fastapi import Header, HTTPException, Request
from app.config import Settings


def verify_admin_token(
    request: Request,
    x_admin_token: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> None:
    settings: Settings = request.app.state.settings
    token = x_admin_token
    if not token and authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    if not token or token != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing admin credentials.")
