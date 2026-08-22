"""
Async SQLAlchemy database setup.

Uses asyncpg driver for non-blocking PostgreSQL access.
The engine and session factory are created once at import time.
"""

import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

logger = logging.getLogger(__name__)

# ─── Engine ───────────────────────────────────────────────────────────────────

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_pre_ping=True,        # detect stale connections before using them
    pool_size=10,
    max_overflow=20,
)

# ─── Session Factory ──────────────────────────────────────────────────────────

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

# ─── Base Class ───────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ─── Dependency ───────────────────────────────────────────────────────────────

async def get_db() -> AsyncSession:
    """
    FastAPI dependency that yields a database session.
    The session is automatically closed (and rolled back on error) after
    the request handler returns.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ─── Table Initialization ─────────────────────────────────────────────────────

async def create_tables() -> None:
    """Create all tables defined in models.py (run once at startup)."""
    try:
        async with engine.begin() as conn:
            # Import models here to ensure they are registered with Base
            import app.models  # noqa: F401
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created / verified.")
    except Exception as exc:
        logger.error("Failed to create database tables: %s", exc)
        raise
