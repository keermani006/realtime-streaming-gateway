"""
Event service handling event persistence in PostgreSQL and Redis publishing.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional, Any
from uuid import UUID

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Event, Client, ConnectionLog
from app.redis_manager import redis_manager
from app.schemas import EventCreate

logger = logging.getLogger(__name__)


class EventService:
    @staticmethod
    async def create_and_publish_event(
        db: AsyncSession,
        event_in: EventCreate,
        instance_id: str,
    ) -> Event:
        """
        1. Persist the event in PostgreSQL.
        2. Publish the event to Redis Pub/Sub for cross-instance distribution.
        """
        payload_str = json.dumps(event_in.payload) if event_in.payload is not None else None

        db_event = Event(
            event_type=event_in.event_type,
            payload=payload_str,
            source_client_id=event_in.source_client_id,
            published_by_instance=instance_id,
        )
        db.add(db_event)
        await db.commit()
        await db.refresh(db_event)

        # Prepare payload for Redis distribution
        broadcast_msg = {
            "type": "event",
            "event_id": str(db_event.id),
            "event_type": db_event.event_type,
            "payload": event_in.payload,
            "source_client_id": db_event.source_client_id,
            "published_by_instance": instance_id,
            "created_at": db_event.created_at.isoformat() if db_event.created_at else datetime.now(timezone.utc).isoformat(),
        }

        # Broadcast via Redis (wrapped so DB commit isn't undone if Redis has a blip)
        try:
            await redis_manager.publish(broadcast_msg)
        except Exception as exc:
            logger.error("Failed to publish event to Redis: %s", exc)

        return db_event

    @staticmethod
    async def get_recent_events(
        db: AsyncSession,
        limit: int = 50,
        event_type: Optional[str] = None,
    ) -> list[Event]:
        """Fetch recent events from PostgreSQL."""
        stmt = select(Event).order_by(Event.created_at.desc()).limit(limit)
        if event_type:
            stmt = stmt.where(Event.event_type == event_type)
        result = await db.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    async def get_total_events_count(db: AsyncSession) -> int:
        """Count total events in PostgreSQL."""
        stmt = select(func.count(Event.id))
        result = await db.execute(stmt)
        return result.scalar() or 0

    @staticmethod
    async def record_client_connect(
        db: AsyncSession,
        client_id: str,
        instance_id: str,
    ) -> None:
        """Register client connection in database and log the connection event."""
        try:
            stmt = select(Client).where(Client.client_id == client_id)
            result = await db.execute(stmt)
            client = result.scalar_one_or_none()

            now = datetime.now(timezone.utc)
            if client is None:
                client = Client(
                    client_id=client_id,
                    total_connections=1,
                    is_currently_connected=True,
                    last_seen=now,
                )
                db.add(client)
            else:
                client.total_connections = (client.total_connections or 0) + 1
                client.is_currently_connected = True
                client.last_seen = now

            log_entry = ConnectionLog(
                client_id=client_id,
                event_type="connected",
                instance_id=instance_id,
                detail="WebSocket connection established",
            )
            db.add(log_entry)
            await db.commit()
        except Exception as exc:
            await db.rollback()
            logger.error("Failed to record client connect for %s: %s", client_id, exc)

    @staticmethod
    async def record_client_disconnect(
        db: AsyncSession,
        client_id: str,
        instance_id: str,
        detail: Optional[str] = None,
    ) -> None:
        """Update client disconnection status in database and log."""
        try:
            stmt = select(Client).where(Client.client_id == client_id)
            result = await db.execute(stmt)
            client = result.scalar_one_or_none()

            now = datetime.now(timezone.utc)
            if client:
                client.is_currently_connected = False
                client.last_seen = now

            log_entry = ConnectionLog(
                client_id=client_id,
                event_type="disconnected",
                instance_id=instance_id,
                detail=detail or "WebSocket connection closed",
            )
            db.add(log_entry)
            await db.commit()
        except Exception as exc:
            await db.rollback()
            logger.error("Failed to record client disconnect for %s: %s", client_id, exc)

    @staticmethod
    async def get_total_clients_count(db: AsyncSession) -> int:
        """Count distinct clients in DB."""
        stmt = select(func.count(Client.id))
        result = await db.execute(stmt)
        return result.scalar() or 0

    @staticmethod
    async def get_client_logs(
        db: AsyncSession,
        client_id: str,
        limit: int = 50,
    ) -> list[ConnectionLog]:
        """Fetch connection logs for a client."""
        stmt = select(ConnectionLog).where(ConnectionLog.client_id == client_id).order_by(ConnectionLog.timestamp.desc()).limit(limit)
        result = await db.execute(stmt)
        return list(result.scalars().all())
