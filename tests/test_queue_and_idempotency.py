from __future__ import annotations

import asyncio
import pytest


@pytest.mark.asyncio
async def test_idempotency_prevents_duplicate_execution_and_billing(client):
    headers = {
        "X-Tenant-ID": "tenant_billing",
        "X-Feature-Name": "invoice_summary",
        "X-Request-ID": "req_idem_1",
        "Idempotency-Key": "idem-key-xyz-999",
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": "Summarize invoice"}],
    }

    r1 = await client.post("/v1/chat/completions", headers=headers, json=payload)
    assert r1.status_code == 200
    id1 = r1.json()["id"]

    headers["X-Request-ID"] = "req_idem_retry_2"
    r2 = await client.post("/v1/chat/completions", headers=headers, json=payload)
    assert r2.status_code == 200
    assert r2.json()["id"] == id1


@pytest.mark.asyncio
async def test_deferrable_work_queues_during_total_outage_and_recovers(client):
    for prov in ("openai", "anthropic", "gemini"):
        await client.post(
            "/admin/chaos",
            headers={"X-Admin-Token": "test-admin-secret"},
            json={"provider": prov, "error_rate": 1.0, "status_code": 503, "duration_seconds": 30},
        )

    resp = await client.post(
        "/v1/chat/completions",
        headers={
            "X-Tenant-ID": "tenant_batch",
            "X-Feature-Name": "nightly_enrichment",
            "X-Request-ID": "req_defer_1",
            "Idempotency-Key": "idem-defer-1",
        },
        json={
            "model": "gpt-4o-mini",
            "request_class": "batch_enrichment",
            "workload_type": "deferrable",
            "messages": [{"role": "user", "content": "Enrich CRM lead"}],
        },
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"

    await client.delete("/admin/chaos", headers={"X-Admin-Token": "test-admin-secret"})
    await asyncio.sleep(0.15)
    processed = await client.app.state.queue_manager.process_due_jobs_once()  # type: ignore[attr-defined]
    assert processed == 1

    status_resp = await client.get("/v1/requests/req_defer_1")
    assert status_resp.status_code == 200
    assert status_resp.json()["status"] == "COMPLETED"
    assert status_resp.json()["response"]["choices"][0]["message"]["content"]
