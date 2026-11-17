from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_rejects_missing_metadata_with_400(client):
    resp = await client.post(
        "/v1/chat/completions",
        json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 400
    assert "tenant_id" in resp.json()["error"]["message"]


@pytest.mark.asyncio
async def test_header_precedence_and_cost_attribution(client):
    resp = await client.post(
        "/v1/chat/completions",
        headers={
            "X-Tenant-ID": "tenant_header",
            "X-Feature-Name": "ticket_triage",
            "X-Request-ID": "req_001",
        },
        json={
            "model": "gpt-4o-mini",
            "tenant_id": "tenant_body_ignored",
            "messages": [{"role": "user", "content": "Classify billing ticket #401"}],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    meta = body["gateway_metadata"]
    assert meta["tenant_id"] == "tenant_header"
    assert meta["feature_name"] == "ticket_triage"
    assert meta["request_id"] == "req_001"
    assert meta["estimated_cost_usd"] > 0

    metrics = await client.get("/metrics")
    assert metrics.status_code == 200
    assert 'tenant_id="tenant_header"' in metrics.text
