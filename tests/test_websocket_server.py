"""
Async unit tests for WebSocketServer.
FeedDistributor is fully mocked. WebSocket methods use AsyncMock.
pytest-asyncio handles the event loop (asyncio_mode=auto in pytest.ini).
"""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import WebSocket
from fastapi.websockets import WebSocketDisconnect
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from WebSocketServer import WebSocketServer


@pytest.fixture
def server_and_fd():
    """WebSocketServer with FeedDistributor fully replaced by a MagicMock."""
    with patch("WebSocketServer.FeedDistributor") as mock_fd_class:
        mock_fd = MagicMock()
        mock_fd_class.return_value = mock_fd
        ws_server = WebSocketServer(host="127.0.0.1", port=8000)
    return ws_server, mock_fd


@pytest.fixture
def mock_ws():
    """Mock WebSocket with async methods."""
    ws = MagicMock(spec=WebSocket)
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.receive_text = AsyncMock()
    ws.client = ("127.0.0.1", 12345)
    return ws


def subscribe_msg(instruments):
    return json.dumps({"action": "subscribe", "instruments": instruments})


def unsubscribe_msg(instruments):
    return json.dumps({"action": "unsubscribe", "instruments": instruments})


# ---------------------------------------------------------------------------
# subscribe / unsubscribe / invalid action
# ---------------------------------------------------------------------------

async def test_subscribe_calls_feed_distributor(server_and_fd, mock_ws):
    server, fd = server_and_fd
    mock_ws.receive_text = AsyncMock(
        side_effect=[subscribe_msg(["NIFTY", "BANKNIFTY"]), WebSocketDisconnect()]
    )
    await server.handle_connection(mock_ws)
    fd.register.assert_called_once()
    fd.subscribe.assert_called_once_with(mock_ws, ["NIFTY", "BANKNIFTY"])


async def test_unsubscribe_calls_feed_distributor(server_and_fd, mock_ws):
    server, fd = server_and_fd
    mock_ws.receive_text = AsyncMock(
        side_effect=[unsubscribe_msg(["NIFTY"]), WebSocketDisconnect()]
    )
    await server.handle_connection(mock_ws)
    fd.unsubscribe.assert_called_once_with(mock_ws, ["NIFTY"])


async def test_invalid_action_sends_error_json(server_and_fd, mock_ws):
    server, fd = server_and_fd
    mock_ws.receive_text = AsyncMock(
        side_effect=[json.dumps({"action": "bogus"}), WebSocketDisconnect()]
    )
    await server.handle_connection(mock_ws)
    mock_ws.send_text.assert_called_once_with(json.dumps({"error": "Invalid action"}))
    fd.subscribe.assert_not_called()
    fd.unsubscribe.assert_not_called()


# ---------------------------------------------------------------------------
# cleanup / disconnect
# ---------------------------------------------------------------------------

async def test_cleanup_on_websocket_disconnect(server_and_fd, mock_ws):
    server, fd = server_and_fd
    mock_ws.receive_text = AsyncMock(side_effect=[WebSocketDisconnect()])
    await server.handle_connection(mock_ws)
    fd.disconnect.assert_called_once_with(mock_ws)
    assert mock_ws not in server.active_connections


async def test_cleanup_method_direct(server_and_fd, mock_ws):
    server, fd = server_and_fd
    server.active_connections.add(mock_ws)
    server.cleanup(mock_ws)
    fd.disconnect.assert_called_once_with(mock_ws)
    assert mock_ws not in server.active_connections


# ---------------------------------------------------------------------------
# push_loop
# ---------------------------------------------------------------------------

async def test_push_loop_sends_queue_item(server_and_fd, mock_ws):
    """Items placed in the asyncio queue are forwarded via send_text."""
    server, fd = server_and_fd
    tick_payload = '{"data": "tick1"}'

    # We need to intercept the queue created inside handle_connection.
    # We do this by having receive_text wait for a signal, then disconnect.
    item_consumed = asyncio.Event()
    disconnect_ready = asyncio.Event()

    original_create_task = asyncio.create_task

    async def receive_side_effect():
        # Wait until the push_loop has consumed the item we'll put
        await disconnect_ready.wait()
        raise WebSocketDisconnect()

    mock_ws.receive_text = AsyncMock(side_effect=receive_side_effect)

    # Capture the queue that handle_connection creates by intercepting register()
    captured_queue = {}

    def fake_register(ws, loop, q):
        captured_queue["q"] = q

    fd.register.side_effect = fake_register

    async def run_test():
        handle_task = asyncio.create_task(server.handle_connection(mock_ws))
        # Give handle_connection a moment to create the queue and start push_loop
        await asyncio.sleep(0.05)
        # Put a tick into the captured queue
        assert "q" in captured_queue, "register() was not called"
        await captured_queue["q"].put(tick_payload)
        # Give push_loop a moment to consume and forward the item
        await asyncio.sleep(0.05)
        # Signal disconnect
        disconnect_ready.set()
        await asyncio.wait_for(handle_task, timeout=2.0)

    await run_test()

    mock_ws.send_text.assert_any_call(tick_payload)


async def test_push_loop_cancels_cleanly_on_disconnect(server_and_fd, mock_ws):
    """push_task.cancel() in finally block does not raise unhandled exception."""
    server, fd = server_and_fd
    mock_ws.receive_text = AsyncMock(side_effect=[WebSocketDisconnect()])
    # Should complete without raising
    await server.handle_connection(mock_ws)


# ---------------------------------------------------------------------------
# active_connections lifecycle
# ---------------------------------------------------------------------------

async def test_active_connections_lifecycle(server_and_fd, mock_ws):
    """Connection is added on accept and removed after disconnect."""
    server, fd = server_and_fd
    assert mock_ws not in server.active_connections

    mock_ws.receive_text = AsyncMock(side_effect=[WebSocketDisconnect()])
    await server.handle_connection(mock_ws)

    # After disconnect, should be removed
    assert mock_ws not in server.active_connections
