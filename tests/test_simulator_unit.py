"""
Unit tests for Simulator helper methods.
ZMQ and DBManager are mocked so no real connections are made.
Private methods are accessed via Python name-mangling (_Simulator__method).
"""
import time
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Simulator import Simulator, SimulatorConfigs, RequestWindow, DbConfigs


@pytest.fixture
def sim():
    """Simulator instance with ZMQ and DBManager fully mocked out."""
    config = SimulatorConfigs(
        pTargetAddress="tcp://127.0.0.1:5555",
        pReqData=[RequestWindow(date="2025-08-06", start_time=0, end_time=10000)],
        pSpeed=1,
        pBatch=None
    )
    with patch("Simulator.zmq.Context"), patch("Simulator.DBManager"):
        instance = Simulator(pSimConfig=config, pDbConfig=DbConfigs())
    return instance


def make_index_row():
    return {
        "instrument": "NSE_INDEX|Nifty 50",
        "ts_ms": 1000,
        "indexLtp": 22000.5,
        "ltt": "09:15:00",
        "cp": 21900.0,
    }


def make_options_row():
    return {
        "instrument": "NSE_OPT|NIFTY25JAN22000CE",
        "ts_ms": 2000,
        "ltp": 150.0,
        "ltt": "09:15:01",
        "ltq": 5,
        "cp": 145.0,
        "up": 0.0,
        "iv": 0.18,
        "delta": 0.45,
        "theta": -0.02,
        "gamma": 0.001,
        "vega": 0.3,
        "atp": 148.0,
        "oi": 100000,
        "poi": 90000,
        "total_buy_qty": 5000,
        "total_sell_qty": 4000,
    }


# ---------------------------------------------------------------------------
# __convert_to_upstox
# ---------------------------------------------------------------------------

def test_convert_index_row(sim):
    row = make_index_row()
    result = sim._Simulator__convert_to_upstox(row)

    feeds = result["feeds"]
    assert "NSE_INDEX|Nifty 50" in feeds
    feed = feeds["NSE_INDEX|Nifty 50"]
    assert "ltpc" in feed
    assert "ff" not in feed
    assert feed["ltpc"]["ltp"] == 22000.5
    assert feed["ltpc"]["cp"] == 21900.0
    assert result["currentTs"] == 1000


def test_convert_options_row(sim):
    row = make_options_row()
    result = sim._Simulator__convert_to_upstox(row)

    feeds = result["feeds"]
    instr = "NSE_OPT|NIFTY25JAN22000CE"
    assert instr in feeds
    feed = feeds[instr]
    assert "ff" in feed
    assert "ltpc" not in feed  # ltpc not at top-level of feed entry

    mff = feed["ff"]["marketFF"]
    assert mff["ltpc"]["ltp"] == 150.0
    assert mff["optionGreeks"]["iv"] == 0.18
    assert mff["optionGreeks"]["delta"] == 0.45
    assert mff["eFeedDetails"]["oi"] == 100000
    assert result["currentTs"] == 2000


# ---------------------------------------------------------------------------
# __fetch_with_retry
# ---------------------------------------------------------------------------

def test_fetch_retry_success_first_attempt(sim):
    mock_df = pd.DataFrame({"col": [1, 2]})
    sim.vDbManager.FetchBatch = MagicMock(return_value=mock_df)

    result = sim._Simulator__fetch_with_retry("2025-08-06", 0, 1000)

    sim.vDbManager.FetchBatch.assert_called_once_with("2025-08-06", 0, 1000)
    assert len(result) == 2


def test_fetch_retry_succeeds_on_third(sim):
    mock_df = pd.DataFrame({"col": [1]})
    sim.vDbManager.FetchBatch = MagicMock(
        side_effect=[Exception("fail1"), Exception("fail2"), mock_df]
    )

    with patch("Simulator.time.sleep") as mock_sleep:
        result = sim._Simulator__fetch_with_retry("2025-08-06", 0, 1000)

    assert sim.vDbManager.FetchBatch.call_count == 3
    assert mock_sleep.call_count == 2
    # Exponential backoff: 2^0=1, 2^1=2
    mock_sleep.assert_any_call(1)
    mock_sleep.assert_any_call(2)
    assert len(result) == 1


def test_fetch_retry_all_fail_returns_empty_df(sim):
    sim.vDbManager.FetchBatch = MagicMock(
        side_effect=[Exception("e1"), Exception("e2"), Exception("e3")]
    )

    with patch("Simulator.time.sleep"):
        result = sim._Simulator__fetch_with_retry("2025-08-06", 0, 1000)

    assert sim.vDbManager.FetchBatch.call_count == Simulator.MAX_FETCH_RETRIES
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# __send_packet pacing
# ---------------------------------------------------------------------------

def test_send_packet_sleeps_when_wait_ms_gt_100(sim):
    sim.vRealStartTime = 0.0
    sim.vSimStartTime = 0

    with patch("Simulator.time.time", return_value=0.0), \
         patch("Simulator.time.sleep") as mock_sleep:
        sim.push_socket.send_string = MagicMock()
        # packet_ts=5000, sim_elapsed=5000ms, real_elapsed=0ms → wait_ms=5000
        sim._Simulator__send_packet({"time_stamp": 5000, "data": []})

    mock_sleep.assert_called_once()
    sleep_arg = mock_sleep.call_args[0][0]
    assert abs(sleep_arg - 5.0) < 0.01


def test_send_packet_no_sleep_when_wait_ms_lte_100(sim):
    sim.vRealStartTime = 0.0
    sim.vSimStartTime = 0

    with patch("Simulator.time.time", return_value=0.0), \
         patch("Simulator.time.sleep") as mock_sleep:
        sim.push_socket.send_string = MagicMock()
        # packet_ts=50, sim_elapsed=50ms → wait_ms=50 ≤ 100
        sim._Simulator__send_packet({"time_stamp": 50, "data": []})

    mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# vRealStartTime lazy-init
# ---------------------------------------------------------------------------

def test_real_start_time_none_at_init(sim):
    """vRealStartTime must be None immediately after construction."""
    assert sim.vRealStartTime is None


def test_real_start_time_set_on_first_packet(sim):
    """vRealStartTime is set (via time.time()) when the first packet is dequeued."""
    fake_packet = {"time_stamp": 1000, "data": []}

    call_count = {"n": 0}

    def fake_get():
        call_count["n"] += 1
        if call_count["n"] == 1:
            return fake_packet
        # Second call: fetch is done, queue empty → exit loop
        sim.vFetchDone.set()
        return None

    sim.vDataQueue.get = fake_get
    sim._Simulator__send_packet = MagicMock()

    with patch("Simulator.time.time", return_value=42.0):
        sim._Simulator__sender_loop()

    assert sim.vRealStartTime == 42.0
