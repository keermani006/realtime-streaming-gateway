"""
Redis Pub/Sub Manager — distributed event propagation.

Responsibilities:
  - Maintain a persistent Redis connection
  - Publish events to a shared channel
  - Subscribe to the channel and forward incoming messages to local clients
  - Reconnect automatically if Redis becomes unavailable (exponential backoff)

This class deliberately does NOT know about WebSocket details.
It calls manager.broadcast() to push events to local clients.
"""

import asyncio
import json
import logging
import time

import redis.asyncio as aioredis
from redis.asyncio.client import PubSub

from app.config import settings

logger = logging.getLogger(__name__)


class RedisManager:
    """
    Manages the connection to Redis and the pub/sub subscription.

    Architecture:
        POST /events  →  RedisManager.publish()  →  Redis channel
                                                         ↓
        All instances ←  _listen_loop()  ←  Redis channel
                ↓
        manager.broadcast()  →  local WebSocket clients
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None
        self._pubsub: PubSub | None = None
        self._listen_task: asyncio.Task | None = None
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Create the Redis client connection."""
        self._redis = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        await self._redis.ping()
        logger.info("Redis connected at %s", settings.REDIS_URL)

    async def start_subscriber(self, broadcast_callback) -> None:
        """
        Subscribe to the gateway channel and launch the listener loop as a
        background asyncio task.  `broadcast_callback` is called with each
        received message dict.
        """
        self._running = True
        self._listen_task = asyncio.create_task(
            self._listen_loop(broadcast_callback),
            name="redis-subscriber",
        )

    async def disconnect(self) -> None:
        """Graceful shutdown."""
        self._running = False

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.close()

        if self._redis:
            await self._redis.aclose()

        logger.info("Redis disconnected.")

    # ── Publish ───────────────────────────────────────────────────────────────

    async def publish(self, message: dict) -> None:
        """
        Publish a message dict to the shared Redis channel.
        All subscriber instances (including this one) will receive it.
        """
        if self._redis is None:
            raise RuntimeError("Redis not connected.")
        payload = json.dumps(message)
        await self._redis.publish(settings.REDIS_CHANNEL, payload)
        logger.debug("Published to %s: %s", settings.REDIS_CHANNEL, payload[:120])

    # ── Subscriber Loop ───────────────────────────────────────────────────────

    async def _listen_loop(self, broadcast_callback) -> None:
        """
        Continuously listen to the Redis channel, calling `broadcast_callback`
        for every valid message.

        Reconnects automatically using exponential backoff if Redis drops.
        """
        delay = settings.RECONNECT_BASE_DELAY
        max_delay = 30.0

        while self._running:
            try:
                self._pubsub = self._redis.pubsub()
                await self._pubsub.subscribe(settings.REDIS_CHANNEL)
                logger.info("Subscribed to Redis channel: %s", settings.REDIS_CHANNEL)

                delay = settings.RECONNECT_BASE_DELAY  # reset on successful connect

                async for raw_message in self._pubsub.listen():
                    if not self._running:
                        break

                    if raw_message["type"] != "message":
                        continue  # skip subscribe/unsubscribe confirmations

                    try:
                        data = json.loads(raw_message["data"])
                        await broadcast_callback(data)
                    except json.JSONDecodeError as exc:
                        logger.warning("Received non-JSON from Redis: %s", exc)
                    except Exception as exc:
                        logger.error("Error in broadcast_callback: %s", exc)

            except asyncio.CancelledError:
                logger.info("Redis listener cancelled.")
                break
            except Exception as exc:
                if not self._running:
                    break
                logger.warning(
                    "Redis subscriber error: %s. Reconnecting in %.1fs…", exc, delay
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

                # Re-establish the Redis connection itself if needed
                try:
                    await self._reconnect_redis()
                except Exception as reconnect_exc:
                    logger.error("Redis reconnect failed: %s", reconnect_exc)

    async def _reconnect_redis(self) -> None:
        """Re-create the Redis client after a connection failure."""
        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass

        self._redis = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        await self._redis.ping()
        logger.info("Redis reconnected.")

    # ── Introspection ─────────────────────────────────────────────────────────

    async def ping(self) -> bool:
        """Return True if Redis is reachable."""
        try:
            if self._redis:
                await self._redis.ping()
                return True
        except Exception:
            pass
        return False


# Module-level singleton
redis_manager = RedisManager()
