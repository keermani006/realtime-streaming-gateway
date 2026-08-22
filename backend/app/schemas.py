"""
Pydantic schemas used for request validation and response serialization.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID
from pydantic import BaseModel, Field


# ─── Event Schemas ────────────────────────────────────────────────────────────

class EventCreate(BaseModel):
    event_type: str = Field(..., min_length=1, max_length=255, examples=["user.signup"])
    payload: Optional[dict[str, Any]] = Field(default=None, examples=[{"user_id": 42}])
    source_client_id: Optional[str] = Field(default=None, max_length=255)


class EventResponse(BaseModel):
    id: UUID
    event_type: str
    payload: Optional[str]        # raw JSON string as stored
    source_client_id: Optional[str]
    published_by_instance: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


# ─── WebSocket Message Schemas ────────────────────────────────────────────────

class WSMessage(BaseModel):
    """Messages sent over the WebSocket connection."""
    type: str                              # "event" | "ping" | "pong" | "ack" | "error"
    data: Optional[dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class WSEventMessage(WSMessage):
    """Outbound event broadcast to connected clients."""
    type: str = "event"
    event_id: Optional[str] = None
    event_type: Optional[str] = None


# ─── Health / Stats Schemas ───────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    instance_id: str
    postgres: str
    redis: str
    connected_clients: int
    uptime_seconds: float


class StatsResponse(BaseModel):
    instance_id: str
    connected_clients: int
    local_client_ids: list[str]
    total_events_published: int
    total_clients_ever: int


# ─── Connection Log Schemas ───────────────────────────────────────────────────

class ConnectionLogResponse(BaseModel):
    id: UUID
    client_id: str
    event_type: str
    instance_id: Optional[str]
    detail: Optional[str]
    timestamp: datetime

    model_config = {"from_attributes": True}
