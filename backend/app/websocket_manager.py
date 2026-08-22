"""
WebSocket Connection Manager — LOCAL state only.

Responsibilities:
  - Accept new WebSocket connections
  - Track active connections per client_id (in memory, per-instance)
  - Send messages to a specific client
  - Broadcast to all locally-connected clients
  - Gracefully remove dead connections

This class deliberately does NOT talk to Redis or PostgreSQL.
Redis pub/sub is handled by RedisManager (redis_manager.py).
DB persistence is handled in websocket.py route.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import WebSocket
from starlette.websockets import WebSocketState

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections LOCAL to this FastAPI instance.

    Design decision: one client_id can only hold ONE active connection at a time.
    If the same client_id reconnects, the previous socket is closed first.
    """

    def __init__(self) -> None:
        # client_id -> WebSocket
        self._connections: dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()

    # ── Connection Lifecycle ──────────────────────────────────────────────────

    async def connect(self, client_id: str, websocket: WebSocket) -> None:
        """Accept the WebSocket and register the client."""
        await websocket.accept()

        async with self._lock:
            # Close any pre-existing connection for this client
            existing = self._connections.get(client_id)
            if existing is not None:
                logger.warning(
                    "Client %s reconnecting — closing old socket.", client_id
                )
                await self._close_socket(existing)

            self._connections[client_id] = websocket

        logger.info("Client %s connected. Total local: %d", client_id, self.count)

    async def disconnect(self, client_id: str) -> None:
        """Remove a client from local tracking (does NOT close the socket)."""
        async with self._lock:
            self._connections.pop(client_id, None)
        logger.info("Client %s disconnected. Total local: %d", client_id, self.count)

    # ── Sending ───────────────────────────────────────────────────────────────

    async def send_personal(self, client_id: str, message: dict) -> bool:
        """
        Send a JSON message to a specific client.
        Returns True on success, False if the send fails (dead connection).
        Dead connections are removed automatically.
        """
        websocket = self._connections.get(client_id)
        if websocket is None:
            return False

        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json(message)
                return True
        except Exception as exc:
            logger.warning("Failed to send to %s: %s — removing dead connection.", client_id, exc)
            await self.disconnect(client_id)

        return False

    async def broadcast(self, message: dict) -> None:
        """
        Broadcast a JSON message to ALL locally-connected clients.
        Dead connections are removed silently.
        """
        # Snapshot the keys to avoid mutation during iteration
        async with self._lock:
            client_ids = list(self._connections.keys())

        tasks = [self.send_personal(cid, message) for cid in client_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed = sum(1 for r in results if r is False or isinstance(r, Exception))
        if failed:
            logger.debug("Broadcast: %d/%d sends failed.", failed, len(client_ids))

    async def send_ping(self, client_id: str) -> bool:
        """Send a ping message so clients know the server is alive."""
        return await self.send_personal(client_id, {
            "type": "ping",
            "timestamp": datetime.utcnow().isoformat(),
        })

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def run_heartbeat(self, interval: int = 30) -> None:
        """
        Background task: ping every connected client every `interval` seconds.
        Stale sockets are culled when the ping fails.
        """
        while True:
            await asyncio.sleep(interval)
            async with self._lock:
                client_ids = list(self._connections.keys())

            if client_ids:
                logger.debug("Heartbeat: pinging %d client(s).", len(client_ids))
                await asyncio.gather(
                    *(self.send_ping(cid) for cid in client_ids),
                    return_exceptions=True,
                )

    # ── Introspection ─────────────────────────────────────────────────────────

    @property
    def count(self) -> int:
        return len(self._connections)

    @property
    def client_ids(self) -> list[str]:
        return list(self._connections.keys())

    def is_connected(self, client_id: str) -> bool:
        return client_id in self._connections

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _close_socket(websocket: WebSocket) -> None:
        """Best-effort graceful close."""
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.close()
        except Exception:
            pass  # Already closed or broken — ignore


# Module-level singleton shared across the application
manager = ConnectionManager()
