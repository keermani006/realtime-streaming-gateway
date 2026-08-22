"""
Integration tests for REST API endpoints.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient):
    """Test /health endpoint returns 200 and expected keys."""
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "postgres" in data
    assert "redis" in data
    assert "connected_clients" in data
    assert "uptime_seconds" in data


@pytest.mark.asyncio
async def test_stats_endpoint(client: AsyncClient):
    """Test /stats endpoint returns gateway metrics."""
    response = await client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert "instance_id" in data
    assert "connected_clients" in data
    assert "total_events_published" in data
    assert "total_clients_ever" in data


@pytest.mark.asyncio
async def test_publish_and_retrieve_events(client: AsyncClient):
    """Test publishing an event via POST /events and reading back via GET /events."""
    payload = {
        "event_type": "user.login",
        "payload": {"user_id": "usr_123", "ip": "127.0.0.1"},
        "source_client_id": "client_alpha",
    }

    # 1. Publish
    post_res = await client.post("/events", json=payload)
    assert post_res.status_code == 201
    event_data = post_res.json()
    assert event_data["event_type"] == "user.login"
    assert event_data["source_client_id"] == "client_alpha"
    assert "id" in event_data

    # 2. Retrieve
    get_res = await client.get("/events")
    assert get_res.status_code == 200
    events_list = get_res.json()
    assert len(events_list) >= 1
    assert events_list[0]["event_type"] == "user.login"

    # 3. Filter by type
    filter_res = await client.get("/events?event_type=user.login")
    assert filter_res.status_code == 200
    assert len(filter_res.json()) >= 1

    filter_none_res = await client.get("/events?event_type=nonexistent.event")
    assert filter_none_res.status_code == 200
    assert len(filter_none_res.json()) == 0
