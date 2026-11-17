from __future__ import annotations

import asyncio
import os
import time
import httpx

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8000")
ADMIN_KEY = os.getenv("ADMIN_API_KEY", "change-me-super-secret-admin-key")


async def main() -> None:
    async with httpx.AsyncClient(base_url=GATEWAY_URL, timeout=10.0) as client:
        total = 0
        succeeded = 0

        async def send_traffic(duration_s: float, phase: str) -> None:
            nonlocal total, succeeded
            end = time.time() + duration_s
            while time.time() < end:
                total += 1
                try:
                    r = await client.post(
                        "/v1/chat/completions",
                        headers={
                            "X-Tenant-ID": "acme_corp",
                            "X-Feature-Name": "support_copilot",
                            "X-Request-ID": f"demo-{phase}-{total}",
                        },
                        json={
                            "model": "gpt-4o-mini",
                            "request_class": "low_latency_classification",
                            "messages": [{"role": "user", "content": "Route ticket"}],
                        },
                    )
                    if r.status_code in (200, 202):
                        succeeded += 1
                except Exception:
                    pass
                await asyncio.sleep(0.1)

        print("[0-15s] Phase 1: Healthy baseline traffic...")
        await send_traffic(15, "healthy")

        print("[15s] Phase 2: Injecting 100% HTTP 500 outage into OpenAI for 25s...")
        await client.post(
            "/admin/chaos",
            headers={"X-Admin-Token": ADMIN_KEY},
            json={"provider": "openai", "error_rate": 1.0, "status_code": 500, "duration_seconds": 25},
        )
        await send_traffic(30, "outage")

        circuits = (await client.get("/admin/circuits", headers={"X-Admin-Token": ADMIN_KEY})).json()
        print("Circuit states during/after outage:", {k: v["state"] for k, v in circuits["providers"].items()})

        print("[45-90s] Phase 3: Observing HALF_OPEN canary probes & automatic recovery to CLOSED...")
        await send_traffic(45, "recovery")

        availability = (succeeded / total) * 100.0 if total else 0.0
        print(f"Completed {succeeded}/{total} requests ({availability:.2f}% availability).")


if __name__ == "__main__":
    asyncio.run(main())
