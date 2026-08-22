"""
Configuration management using Pydantic Settings.
All values are read from environment variables with sensible defaults.
Supports local development, Docker Compose, and cloud platforms
(Render, Railway, Fly.io, Supabase, Neon, Upstash).
"""

import os
from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "Real-Time Streaming Gateway"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", "8000"))

    # Explicit Database & Redis URLs (set by cloud PaaS like Render/Railway/Supabase)
    DATABASE_URL_ENV: Optional[str] = os.getenv("DATABASE_URL")
    REDIS_URL_ENV: Optional[str] = os.getenv("REDIS_URL")

    # PostgreSQL individual params (fallback if DATABASE_URL not set)
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "streaming_gateway"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"

    # Redis individual params (fallback if REDIS_URL not set)
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: Optional[str] = None
    REDIS_CHANNEL: str = "gateway:events"

    # CORS
    CORS_ORIGINS: str = "*"

    # WebSocket
    HEARTBEAT_INTERVAL: int = 30  # seconds between pings
    HEARTBEAT_TIMEOUT: int = 10   # seconds to wait for pong

    # Connection
    MAX_RECONNECT_ATTEMPTS: int = 5
    RECONNECT_BASE_DELAY: float = 1.0  # seconds (exponential backoff base)

    @property
    def DATABASE_URL(self) -> str:
        """
        Normalize database URL to use asyncpg driver (postgresql+asyncpg://).
        PaaS platforms like Render/Railway/Supabase provide postgres:// or postgresql://.
        """
        raw_url = self.DATABASE_URL_ENV or os.getenv("DATABASE_URL")
        if raw_url:
            if raw_url.startswith("postgres://"):
                raw_url = raw_url.replace("postgres://", "postgresql+asyncpg://", 1)
            elif raw_url.startswith("postgresql://") and not raw_url.startswith("postgresql+asyncpg://"):
                raw_url = raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            return raw_url

        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def REDIS_URL(self) -> str:
        """Return Redis connection string (supports redis:// and rediss:// TLS)."""
        raw_url = self.REDIS_URL_ENV or os.getenv("REDIS_URL")
        if raw_url:
            return raw_url

        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}"

    @property
    def CORS_ORIGINS_LIST(self) -> list[str]:
        """Split comma-separated origins."""
        if self.CORS_ORIGINS == "*":
            return ["*"]
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",") if origin.strip()]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance — only parsed once per process."""
    return Settings()


settings = get_settings()
