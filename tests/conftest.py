from __future__ import annotations

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        mock_providers_enabled=True,
        admin_api_key="test-admin-secret",
        cb_min_sample_size=3,
        cb_error_rate_threshold=0.5,
        cb_p95_latency_threshold_ms=1000.0,
        cb_open_duration_seconds=2,
        cb_half_open_canary_rate=1.0,
        cb_required_successes_to_close=2,
        cb_required_failures_to_reopen=1,
        queue_base_backoff_seconds=0.05,
        queue_max_backoff_seconds=0.2,
    )


@pytest.fixture
async def fake_redis():
    server = fakeredis.aioredis.FakeRedis()
    yield server
    await server.aclose()


@pytest.fixture
async def client(test_settings: Settings, fake_redis):
    app = create_app(settings=test_settings, redis_client=fake_redis, start_worker=False)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            ac.app = app  # type: ignore[attr-defined]
            yield ac
