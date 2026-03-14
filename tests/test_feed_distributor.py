"""
Unit tests for FeedDistributor.
Singleton is reset between every test via an autouse fixture.
ZMQ is mocked to prevent real socket creation.
"""
import json
import threading
import pytest
from unittest.mock import patch, MagicMock, call
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from FeedDistributor import FeedDistributor


@pytest.fixture(autouse=True)
def reset_singleton():
    """Destroy any existing FeedDistributor singleton before and after each test."""
    FeedDistributor._instance = None
    yield
    FeedDistributor._instance = None


@pytest.fixture
def fd():
    """Configured FeedDistributor with ZMQ mocked."""
    with patch("FeedDistributor.zmq.Context"):
        instance = FeedDistributor()
        instance.config(zmq_pull_addr="tcp://127.0.0.1:5555")
    return instance


def make_upstox_tick(instrument: str) -> dict:
    """Return an Upstox-format tick dict for a given instrument."""
    return {"feeds": {instrument: {}}, "currentTs": 1000}


# ---------------------------------------------------------------------------
# _run_once helper
# ---------------------------------------------------------------------------

def _run_once(fd, tick_dicts: list):
    """Drive __run for exactly one recv() call (a JSON array of tick dicts).

    Sets fd.running = False before the second recv() so the while loop exits
    cleanly after the outer except block (the except only logs when running=True).
    """
    call_count = {"n": 0}
    raw = json.dumps(tick_dicts).encode()

    def fake_recv():
        call_count["n"] += 1
        if call_count["n"] == 1:
            return raw
        fd.running = False
        raise Exception("stop")

    fd.pull_socket.recv = fake_recv
    fd.running = True
    fd._FeedDistributor__run()


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

def test_singleton_same_instance():
    """Two FeedDistributor() calls return the identical object."""
    with patch("FeedDistributor.zmq.Context"):
        fd1 = FeedDistributor()
        fd2 = FeedDistributor()
    assert fd1 is fd2


def test_singleton_reset_between_tests():
    """After reset_singleton fixture, a fresh instance is created."""
    assert FeedDistributor._instance is None
    with patch("FeedDistributor.zmq.Context"):
        fd = FeedDistributor()
    assert fd is not None


# ---------------------------------------------------------------------------
# subscribe / unsubscribe / disconnect
# ---------------------------------------------------------------------------

def test_subscribe_populates_map(fd):
    ws = MagicMock()
    fd.subscribe(ws, ["NSE_INDEX|Nifty 50", "NSE_OPT|NIFTY24800CE"])
    assert ws in fd.vInsToConn["NSE_INDEX|Nifty 50"]
    assert ws in fd.vInsToConn["NSE_OPT|NIFTY24800CE"]


def test_unsubscribe_removes_and_cleans_empty(fd):
    ws = MagicMock()
    fd.subscribe(ws, ["NSE_INDEX|Nifty 50"])
    fd.unsubscribe(ws, ["NSE_INDEX|Nifty 50"])
    assert "NSE_INDEX|Nifty 50" not in fd.vInsToConn


def test_unsubscribe_leaves_other_subscribers(fd):
    ws1, ws2 = MagicMock(), MagicMock()
    fd.subscribe(ws1, ["NSE_INDEX|Nifty 50"])
    fd.subscribe(ws2, ["NSE_INDEX|Nifty 50"])
    fd.unsubscribe(ws1, ["NSE_INDEX|Nifty 50"])
    assert "NSE_INDEX|Nifty 50" in fd.vInsToConn
    assert ws2 in fd.vInsToConn["NSE_INDEX|Nifty 50"]
    assert ws1 not in fd.vInsToConn["NSE_INDEX|Nifty 50"]


def test_disconnect_cleans_all_mappings(fd):
    ws = MagicMock()
    mock_loop, mock_q = MagicMock(), MagicMock()
    fd.subscribe(ws, ["NSE_INDEX|Nifty 50", "NSE_OPT|NIFTY24800CE"])
    fd.register(ws, mock_loop, mock_q)
    fd.disconnect(ws)
    assert "NSE_INDEX|Nifty 50" not in fd.vInsToConn
    assert "NSE_OPT|NIFTY24800CE" not in fd.vInsToConn
    assert ws not in fd.connToQueue


# ---------------------------------------------------------------------------
# __run routing (JSON array format, real instrument names with | in them)
# ---------------------------------------------------------------------------

def test_run_routes_tick_to_correct_queue(fd):
    """Valid Upstox JSON tick array → loop.call_soon_threadsafe called with tick JSON."""
    ws = MagicMock()
    mock_loop, mock_q = MagicMock(), MagicMock()
    fd.subscribe(ws, ["NSE_INDEX|Nifty 50"])
    fd.register(ws, mock_loop, mock_q)

    tick_dict = make_upstox_tick("NSE_INDEX|Nifty 50")
    _run_once(fd, [tick_dict])

    mock_loop.call_soon_threadsafe.assert_called_once_with(
        mock_q.put_nowait, json.dumps(tick_dict)
    )


def test_run_invalid_json_no_crash(fd):
    """A non-JSON ZMQ message does not propagate an exception."""
    # Bypass _run_once helper since this needs raw bytes, not a list
    call_count = {"n": 0}

    def fake_recv():
        call_count["n"] += 1
        if call_count["n"] == 1:
            return b"not valid json"
        fd.running = False
        raise Exception("stop")

    fd.pull_socket.recv = fake_recv
    fd.running = True
    fd._FeedDistributor__run()
    # No assertion needed — reaching here means no crash


def test_run_unknown_instrument_no_error(fd):
    """Tick for an unsubscribed instrument produces no call_soon_threadsafe call."""
    ws = MagicMock()
    mock_loop, mock_q = MagicMock(), MagicMock()
    fd.subscribe(ws, ["NSE_INDEX|Nifty 50"])
    fd.register(ws, mock_loop, mock_q)

    tick_dict = make_upstox_tick("NSE_OPT|NIFTY24800CE")  # not subscribed
    _run_once(fd, [tick_dict])

    mock_loop.call_soon_threadsafe.assert_not_called()


def test_run_multiple_ticks_json_array(fd):
    """Two ticks in one JSON array → call_soon_threadsafe called twice.
    Real instrument names with '|' work correctly now that the pipe
    delimiter has been replaced by a JSON array envelope.
    """
    ws = MagicMock()
    mock_loop, mock_q = MagicMock(), MagicMock()
    fd.subscribe(ws, ["NSE_INDEX|Nifty 50", "NSE_OPT|NIFTY24800CE"])
    fd.register(ws, mock_loop, mock_q)

    tick1 = make_upstox_tick("NSE_INDEX|Nifty 50")
    tick2 = make_upstox_tick("NSE_OPT|NIFTY24800CE")
    _run_once(fd, [tick1, tick2])

    assert mock_loop.call_soon_threadsafe.call_count == 2
