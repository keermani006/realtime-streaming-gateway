"""
Main FastAPI Application for Real-Time Streaming Gateway.

Handles lifecycle management:
  - Database schema initialization
  - Redis connection & Pub/Sub subscription loop
  - Periodic WebSocket heartbeat task
  - Graceful shutdown
"""

import asyncio
import logging
import os
import socket
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import create_tables
from app.redis_manager import redis_manager
from app.websocket_manager import manager
from app.routes import websocket, events, health

# Configure Logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application Lifespan Context Manager.
    Initializes background workers and connections on startup, and cleans up on shutdown.
    """
    # 1. Instance Identification (useful for multi-container horizontal scale demos)
    instance_name = os.getenv("INSTANCE_NAME")
    if not instance_name:
        hostname = socket.gethostname()
        short_id = str(uuid.uuid4())[:8]
        instance_name = f"{hostname}-{short_id}"
    app.state.instance_id = instance_name
    app.state.start_time = time.time()
    logger.info("Initializing Real-Time Gateway instance: %s", instance_name)

    # 2. Initialize Database Schema
    try:
        await create_tables()
    except Exception as exc:
        logger.warning("Database setup warning (will retry on incoming requests): %s", exc)

    # 3. Initialize Redis & Pub/Sub Subscription
    try:
        await redis_manager.connect()
        # Wire Redis subscriber directly to the local connection manager broadcast
        await redis_manager.start_subscriber(broadcast_callback=manager.broadcast)
        logger.info("Redis Pub/Sub listener initialized.")
    except Exception as exc:
        logger.error("Failed to connect to Redis during startup: %s", exc)

    # 4. Start WebSocket Heartbeat Task
    heartbeat_task = asyncio.create_task(
        manager.run_heartbeat(interval=settings.HEARTBEAT_INTERVAL),
        name="ws-heartbeat",
    )

    logger.info("Real-Time Streaming Gateway is online and ready.")

    yield

    # Shutdown sequence
    logger.info("Shutting down Real-Time Gateway instance: %s", instance_name)
    heartbeat_task.cancel()
    try:
        await heartbeat_task
    except asyncio.CancelledError:
        pass

    await redis_manager.disconnect()
    logger.info("Shutdown complete.")


def create_app() -> FastAPI:
    """Application factory."""
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description=(
            "Distributed Real-Time WebSocket Streaming Gateway powered by FastAPI, "
            "Redis Pub/Sub, and PostgreSQL."
        ),
        lifespan=lifespan,
    )

    # CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS_LIST,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Register Routers
    app.include_router(health.router)
    app.include_router(events.router)
    app.include_router(websocket.router)

    return app


app = create_app()
