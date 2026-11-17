from __future__ import annotations

import asyncio
import pytest


@pytest.mark.asyncio
async def test_content_filter_does_not_trip_breaker(client):
    for i in range(5):
        resp = await client.post(
            "/v1/chat/completions",
            headers={
                "X-Tenant-ID": "t1",
                "X-Feature-Name": "safety_check",
                "X-Request-ID": f"req_cf_{i}",
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "[TRIGGER_CONTENT_FILTER]"}],
            },
        )
        assert resp.status_code == 400

    circuits = await client.get("/admin/circuits", headers={"X-Admin-Token": "test-admin-secret"})
    assert circuits.json()["providers"]["openai"]["state"] == "CLOSED"


@pytest.mark.asyncio
async def test_chaos_trips_breaker_reroutes_and_self_heals(client):
    chaos_resp = await client.post(
        "/admin/chaos",
        headers={"X-Admin-Token": "test-admin-secret"},
        json={"provider": "openai", "error_rate": 1.0, "status_code": 500, "duration_seconds": 30},
    )
    assert chaos_resp.status_code == 200

    for i in range(4):
        r = await client.post(
            "/v1/chat/completions",
            headers={
                "X-Tenant-ID": "t1",
                "X-Feature-Name": "routing_test",
                "X-Request-ID": f"req_ll_{i}",
            },
            json={
                "model": "gpt-4o-mini",
                "request_class": "low_latency_classification",
                "messages": [{"role": "user", "content": "Classify"}],
            },
        )
        assert r.status_code == 200

    circuits = await client.get("/admin/circuits", headers={"X-Admin-Token": "test-admin-secret"})
    assert circuits.json()["providers"]["openai"]["state"] == "OPEN"

    await client.delete("/admin/chaos", headers={"X-Admin-Token": "test-admin-secret"})
    await asyncio.sleep(2.1)

    for i in range(2):
        r = await client.post(
            "/v1/chat/completions",
            headers={
                "X-Tenant-ID": "t1",
                "X-Feature-Name": "recovery_test",
                "X-Request-ID": f"req_rec_{i}",
            },
            json={
                "model": "gpt-4o-mini",
                "request_class": "low_latency_classification",
                "messages": [{"role": "user", "content": "Recover"}],
            },
        )
        assert r.status_code == 200
        assert r.json()["gateway_metadata"]["provider"] == "openai"

    circuits_after = await client.get("/admin/circuits", headers={"X-Admin-Token": "test-admin-secret"})
    assert circuits_after.json()["providers"]["openai"]["state"] == "CLOSED"
