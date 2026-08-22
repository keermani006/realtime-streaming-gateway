"""
WebSocket router handling client connection lifecycle, messages, and ping/pong.
"""

import json
import logging
from typing import Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, AsyncSessionLocal
from app.websocket_manager import manager
from app.services.event_service import EventService
from app.schemas import EventCreate

logger = logging.getLogger(__name__)

router = APIRouter(tags=["WebSocket"])


@router.websocket("/ws/{client_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: str,
):
    """
    WebSocket gateway endpoint.
    Maintains persistent connections, handles incoming frames (ping/pong or client events),
    and records connection lifecycle to PostgreSQL.
    """
    instance_id = getattr(websocket.app.state, "instance_id", "instance-unknown")

    # Accept connection & register locally
    await manager.connect(client_id, websocket)

    # Record connect in database asynchronously
    async with AsyncSessionLocal() as db:
        await EventService.record_client_connect(db, client_id, instance_id)

    # Send welcome / connected acknowledgment
    await manager.send_personal(client_id, {
        "type": "ack",
        "status": "connected",
        "client_id": client_id,
        "instance_id": instance_id,
        "message": f"Connected to Real-Time Gateway on instance {instance_id}"
    })

    try:
        while True:
            # Receive text / json from the client
            data = await websocket.receive_text()
            try:
                msg_json = json.loads(data)
            except json.JSONDecodeError:
                await manager.send_personal(client_id, {
                    "type": "error",
                    "detail": "Invalid JSON format"
                })
                continue

            msg_type = msg_json.get("type", "message")

            if msg_type == "ping":
                # Heartbeat reply
                await manager.send_personal(client_id, {
                    "type": "pong",
                    "client_id": client_id,
                    "instance_id": instance_id,
                })
            elif msg_type == "publish":
                # Allow clients to also publish events directly over WebSocket
                event_type = msg_json.get("event_type", "client.message")
                payload = msg_json.get("payload", {})
                event_in = EventCreate(
                    event_type=event_type,
                    payload=payload,
                    source_client_id=client_id,
                )
                async with AsyncSessionLocal() as db:
                    await EventService.create_and_publish_event(db, event_in, instance_id)
            else:
                # Echo / Ack custom message
                await manager.send_personal(client_id, {
                    "type": "ack",
                    "received": msg_json
                })

    except WebSocketDisconnect:
        logger.info("Client %s disconnected cleanly.", client_id)
        await manager.disconnect(client_id)
        async with AsyncSessionLocal() as db:
            await EventService.record_client_disconnect(db, client_id, instance_id, detail="Client disconnected cleanly")
    except Exception as exc:
        logger.warning("Client %s connection error: %s", client_id, exc)
        await manager.disconnect(client_id)
        async with AsyncSessionLocal() as db:
            await EventService.record_client_disconnect(db, client_id, instance_id, detail=f"Error: {str(exc)}")
