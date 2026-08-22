"""
Pytest configuration and fixtures.
"""

import asyncio
import os
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from unittest.mock import AsyncMock, patch

# Set test environment
os.environ["DEBUG"] = "true"
os.environ["POSTGRES_DB"] = "test_gateway"

from app.main import app
from app.database import Base, get_db
from app.models import Client, Event, ConnectionLog
from app.websocket_manager import ConnectionManager
from app.redis_manager import RedisManager

# Use SQLite in-memory for fast unit testing
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestingSessionLocal = async_sessionmaker(
    bind=test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
async def init_test_db():
    """Create test DB schema before each test and drop after."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def db_session():
    """Yield a clean test database session."""
    async with TestingSessionLocal() as session:
        yield session


@pytest.fixture
async def client(db_session: AsyncSession):
    """FastAPI async test client with overridden DB session and mocked Redis."""
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    # Mock Redis publish
    with patch("app.redis_manager.redis_manager.publish", new_callable=AsyncMock) as mock_pub, \
         patch("app.redis_manager.redis_manager.ping", new_callable=AsyncMock, return_value=True):
        
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac

    app.dependency_overrides.clear()
