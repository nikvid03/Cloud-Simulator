"""
Unit tests for DataQueue — pure Python, no mocks needed.
DataQueue must have .start() called before any .put().
"""
import queue
import threading
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Simulator import DataQueue


@pytest.fixture
def dq():
    """Non-batch mode DataQueue starting at ts=1000."""
    q = DataQueue()
    q.start(batch_size=None, start_ts=1000)
    return q


@pytest.fixture
def dq_batch():
    """Batch mode DataQueue with batch_size=1000, starting at ts=1000."""
    q = DataQueue()
    q.start(batch_size=1000, start_ts=1000)
    return q


def make_tick(ts):
    return {"currentTs": ts, "feeds": {}}


# ---------------------------------------------------------------------------
# Non-batch mode tests
# ---------------------------------------------------------------------------

def test_same_timestamp_accumulates(dq):
    """Two ticks with same ts accumulate in vCurrDataArray; queue stays empty."""
    dq.put(make_tick(1000))
    dq.put(make_tick(1000))
    assert len(dq.vCurrDataArray) == 2
    assert dq.vQueue.empty()


def test_different_timestamp_flushes(dq):
    """Second tick at a new ts flushes the first group into the queue."""
    dq.put(make_tick(1000))
    dq.put(make_tick(2000))
    assert dq.vQueue.qsize() == 1
    flushed = dq.vQueue.get_nowait()
    assert flushed["time_stamp"] == 1000
    assert len(flushed["data"]) == 1
    # The second tick is now in the accumulator
    assert len(dq.vCurrDataArray) == 1
    assert dq.vCurrDataArray[0]["currentTs"] == 2000


def test_stale_tick_dropped_non_batch(dq):
    """Tick with currentTs < vNextTs is silently dropped."""
    dq.put(make_tick(1000))  # vNextTs becomes 1000
    dq.put(make_tick(500))   # stale
    assert len(dq.vCurrDataArray) == 1
    assert dq.vQueue.empty()


def test_end_flushes_remaining(dq):
    """end() pushes accumulated ticks into the queue and clears the array."""
    dq.put(make_tick(1000))
    assert dq.vQueue.empty()
    dq.end()
    assert dq.vQueue.qsize() == 1
    assert dq.vCurrDataArray == []


def test_end_on_empty_is_noop(dq):
    """end() on an empty accumulator raises no error and leaves queue empty."""
    dq.end()
    assert dq.vQueue.empty()
    assert dq.vCurrDataArray == []


def test_get_returns_flushed_packet(dq):
    """get() dequeues the oldest flushed packet with correct structure."""
    dq.put(make_tick(1000))
    dq.put(make_tick(2000))  # triggers flush of ts=1000
    packet = dq.get()
    assert packet is not None
    assert packet["time_stamp"] == 1000
    assert isinstance(packet["data"], list)
    assert len(packet["data"]) == 1


def test_get_timeout_returns_none():
    """get() on an empty queue returns None after timeout (not a hang)."""
    q = DataQueue()
    q.start(batch_size=None, start_ts=0)
    original = DataQueue.RETRIEVAL_TIMEOUT
    DataQueue.RETRIEVAL_TIMEOUT = 0.1  # shorten to 100ms for the test
    try:
        result = q.get()
        assert result is None
    finally:
        DataQueue.RETRIEVAL_TIMEOUT = original


def test_put_populates_correct_batch_dict_keys(dq):
    """Flushed dict contains exactly 'time_stamp' and 'data' keys."""
    dq.put(make_tick(1000))
    dq.put(make_tick(2000))  # flushes 1000
    packet = dq.vQueue.get_nowait()
    assert set(packet.keys()) == {"time_stamp", "data"}


def test_queue_full_retry_resolves():
    """__put retries until space is freed; background drain unblocks it."""
    q = DataQueue()
    q.start(batch_size=None, start_ts=0)
    # Shrink the queue to 1 slot and pre-fill it
    q.vQueue = queue.Queue(maxsize=1)
    q.vQueue.put("filler")

    original_insertion = DataQueue.INSERTION_TIMEOUT
    DataQueue.INSERTION_TIMEOUT = 0.05  # short retry interval

    results = []

    def drain_after_delay():
        import time
        time.sleep(0.15)
        try:
            q.vQueue.get_nowait()
        except Exception:
            pass

    drainer = threading.Thread(target=drain_after_delay, daemon=True)
    drainer.start()

    try:
        # end() will call __put internally; should eventually succeed
        q.vCurrDataArray = [make_tick(0)]
        q.end()
        results.append("ok")
    finally:
        DataQueue.INSERTION_TIMEOUT = original_insertion

    drainer.join(timeout=2)
    assert results == ["ok"]


# ---------------------------------------------------------------------------
# Batch mode tests
# ---------------------------------------------------------------------------

def test_batch_mode_stale_drop(dq_batch):
    """In batch mode, tick with currentTs <= (vNextTs - batch_size) is dropped.
    vNextTs=1000, batch_size=1000 → stale threshold: currentTs <= 0.
    """
    dq_batch.put(make_tick(0))  # 0 <= 1000-1000=0 → stale
    assert dq_batch.vCurrDataArray == []
    assert dq_batch.vQueue.empty()


def test_batch_mode_advances_multiple_buckets(dq_batch):
    """A large ts jump creates and flushes multiple batch buckets.

    vNextTs=1000, batch_size=1000. Condition: currentTs > (vNextTs - batch_size).
    ts=3500 advances vNextTs through 1000→2000→3000→4000→5000 (4 flushes).
    The tick itself is then accumulated in the ts=5000 bucket.
    """
    dq_batch.put(make_tick(3500))
    # 4 empty buckets flushed: ts=1000, ts=2000, ts=3000, ts=4000
    assert dq_batch.vQueue.qsize() == 4
    ts_values = [dq_batch.vQueue.get_nowait()["time_stamp"] for _ in range(4)]
    assert ts_values == [1000, 2000, 3000, 4000]
