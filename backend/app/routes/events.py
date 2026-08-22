"""
REST API routes for events and client history.
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas import EventCreate, EventResponse, ConnectionLogResponse
from app.services.event_service import EventService

router = APIRouter(prefix="/events", tags=["Events"])


@router.post(
    "",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Publish an event",
    description="Persists an event in PostgreSQL and broadcasts it via Redis Pub/Sub to all connected WebSocket clients across all gateway instances."
)
async def publish_event(
    event_in: EventCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    instance_id = getattr(request.app.state, "instance_id", "unknown-instance")
    event = await EventService.create_and_publish_event(
        db=db,
        event_in=event_in,
        instance_id=instance_id,
    )
    return event


@router.get(
    "",
    response_model=list[EventResponse],
    summary="Retrieve recent events",
    description="Fetches recently stored events from PostgreSQL with optional filtering by event_type."
)
async def get_events(
    limit: int = 50,
    event_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    if limit > 200:
        limit = 200
    events = await EventService.get_recent_events(db=db, limit=limit, event_type=event_type)
    return events


@router.get(
    "/clients/{client_id}/logs",
    response_model=list[ConnectionLogResponse],
    summary="Get client connection history",
    description="Retrieves connect/disconnect timestamps and audit logs for a specific client."
)
async def get_client_connection_logs(
    client_id: str,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    logs = await EventService.get_client_logs(db=db, client_id=client_id, limit=limit)
    return logs
