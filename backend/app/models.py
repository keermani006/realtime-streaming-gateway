"""
SQLAlchemy ORM models.

Three tables:
  - clients         : registered WebSocket clients
  - connection_logs : connect/disconnect history per client
  - events          : published events (persisted for audit / retrieval)
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    Boolean,
    Integer,
    ForeignKey,
    Uuid,
    func,
)
import uuid

from app.database import Base


class Client(Base):
    __tablename__ = "clients"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(String(255), unique=True, nullable=False, index=True)
    first_seen = Column(DateTime(timezone=True), server_default=func.now())
    last_seen = Column(DateTime(timezone=True), onupdate=func.now())
    total_connections = Column(Integer, default=0)
    is_currently_connected = Column(Boolean, default=False)

    def __repr__(self) -> str:
        return f"<Client client_id={self.client_id}>"


class ConnectionLog(Base):
    __tablename__ = "connection_logs"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(String(255), ForeignKey("clients.client_id"), nullable=False, index=True)
    event_type = Column(String(50), nullable=False)  # "connected" | "disconnected" | "error"
    instance_id = Column(String(255), nullable=True)  # which FastAPI instance handled this
    detail = Column(Text, nullable=True)              # optional extra context
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"<ConnectionLog client_id={self.client_id} event={self.event_type}>"


class Event(Base):
    __tablename__ = "events"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String(255), nullable=False, index=True)
    payload = Column(Text, nullable=True)          # JSON string
    source_client_id = Column(String(255), nullable=True)
    published_by_instance = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"<Event type={self.event_type} id={self.id}>"
