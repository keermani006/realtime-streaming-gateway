"""
Health and statistics endpoints.
"""

import time
from fastapi import APIRouter, Depends, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.redis_manager import redis_manager
from app.websocket_manager import manager
from app.services.event_service import EventService
from app.schemas import HealthResponse, StatsResponse

router = APIRouter(tags=["Health & Stats"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Inspects PostgreSQL and Redis connectivity, uptime, and active local connections."
)
async def health_check(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    instance_id = getattr(request.app.state, "instance_id", "unknown-instance")
    start_time = getattr(request.app.state, "start_time", time.time())
    uptime = time.time() - start_time

    # Check Postgres
    postgres_status = "healthy"
    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        postgres_status = "unhealthy"

    # Check Redis
    redis_healthy = await redis_manager.ping()
    redis_status = "healthy" if redis_healthy else "unhealthy"

    overall_status = "ok" if (postgres_status == "healthy" and redis_status == "healthy") else "degraded"

    return HealthResponse(
        status=overall_status,
        instance_id=instance_id,
        postgres=postgres_status,
        redis=redis_status,
        connected_clients=manager.count,
        uptime_seconds=round(uptime, 2),
    )


@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Gateway statistics",
    description="Returns metrics on active local WebSocket clients, total registered clients, and total published events."
)
async def gateway_stats(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    instance_id = getattr(request.app.state, "instance_id", "unknown-instance")
    total_events = await EventService.get_total_events_count(db)
    total_clients = await EventService.get_total_clients_count(db)

    return StatsResponse(
        instance_id=instance_id,
        connected_clients=manager.count,
        local_client_ids=manager.client_ids,
        total_events_published=total_events,
        total_clients_ever=total_clients,
    )
