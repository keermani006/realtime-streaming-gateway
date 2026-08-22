"""
Unit and functional tests for WebSocket ConnectionManager.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from starlette.websockets import WebSocketState

from app.websocket_manager import ConnectionManager


@pytest.mark.asyncio
async def test_connection_manager_lifecycle():
    """Test connect, broadcast, send_personal, and disconnect."""
    cm = ConnectionManager()
    assert cm.count == 0

    # Create mock WebSocket
    mock_ws1 = AsyncMock()
    mock_ws1.client_state = WebSocketState.CONNECTED

    mock_ws2 = AsyncMock()
    mock_ws2.client_state = WebSocketState.CONNECTED

    # Connect clients
    await cm.connect("client_1", mock_ws1)
    await cm.connect("client_2", mock_ws2)

    assert cm.count == 2
    assert cm.is_connected("client_1")
    assert cm.is_connected("client_2")
    assert "client_1" in cm.client_ids
    assert "client_2" in cm.client_ids

    # Send personal message
    success = await cm.send_personal("client_1", {"msg": "hello client 1"})
    assert success is True
    mock_ws1.send_json.assert_awaited_once_with({"msg": "hello client 1"})

    # Broadcast message
    await cm.broadcast({"event": "system_alert"})
    mock_ws1.send_json.assert_awaited_with({"event": "system_alert"})
    mock_ws2.send_json.assert_awaited_with({"event": "system_alert"})

    # Disconnect client
    await cm.disconnect("client_1")
    assert cm.count == 1
    assert not cm.is_connected("client_1")
    assert cm.is_connected("client_2")


@pytest.mark.asyncio
async def test_connection_manager_dead_socket_pruning():
    """Test that failed sends automatically prune dead connections."""
    cm = ConnectionManager()

    dead_ws = AsyncMock()
    dead_ws.client_state = WebSocketState.CONNECTED
    dead_ws.send_json.side_effect = Exception("Broken pipe")

    await cm.connect("dead_client", dead_ws)
    assert cm.count == 1

    success = await cm.send_personal("dead_client", {"test": "data"})
    assert success is False
    # Should have been pruned
    assert cm.count == 0
    assert not cm.is_connected("dead_client")
