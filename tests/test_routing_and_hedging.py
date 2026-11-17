from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_request_class_specific_preference_lists(client):
    r_low = await client.post(
        "/v1/chat/completions",
        headers={"X-Tenant-ID": "t1", "X-Feature-Name": "f1", "X-Request-ID": "r1"},
        json={
            "model": "gpt-4o-mini",
            "request_class": "low_latency_classification",
            "messages": [{"role": "user", "content": "Fast classify"}],
        },
    )
    assert r_low.json()["gateway_metadata"]["provider"] == "openai"

    r_long = await client.post(
        "/v1/chat/completions",
        headers={"X-Tenant-ID": "t1", "X-Feature-Name": "f1", "X-Request-ID": "r2"},
        json={
            "model": "gpt-4o-mini",
            "request_class": "long_form_generation",
            "messages": [{"role": "user", "content": "Write essay"}],
        },
    )
    assert r_long.json()["gateway_metadata"]["provider"] == "anthropic"


@pytest.mark.asyncio
async def test_hedging_speculative_secondary_wins_slow_primary(client):
    await client.post(
        "/admin/chaos",
        headers={"X-Admin-Token": "test-admin-secret"},
        json={"provider": "openai", "latency_ms": 1500, "error_rate": 0.0, "duration_seconds": 30},
    )

    resp = await client.post(
        "/v1/chat/completions",
        headers={"X-Tenant-ID": "t1", "X-Feature-Name": "hedge_test", "X-Request-ID": "r_hedge"},
        json={
            "model": "gpt-4o-mini",
            "request_class": "low_latency_classification",
            "messages": [{"role": "user", "content": "Urgent intent"}],
        },
    )
    assert resp.status_code == 200
    meta = resp.json()["gateway_metadata"]
    assert meta["hedged"] is True
    assert meta["provider"] == "gemini"
    assert meta["latency_ms"] < 1200
