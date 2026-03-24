import pytest
from zoneinfo import ZoneInfo
from datetime import datetime
from DataLossTracker import DataLossTracker, _window_label


def _ist_ms(hour, minute, second=0):
    """Return epoch-ms for 2024-10-03 HH:MM:SS IST — makes test inputs readable."""
    ist = ZoneInfo("Asia/Kolkata")
    dt = datetime(2024, 10, 3, hour, minute, second, tzinfo=ist)
    return int(dt.timestamp() * 1000)


# ── _window_label ─────────────────────────────────────────────────────────────

def test_window_label_first_half_hour():
    """09:16 IST rounds down to 09:00."""
    assert _window_label(_ist_ms(9, 16)) == "09:00"


def test_window_label_second_half_hour():
    """09:46 IST rounds down to 09:30."""
    assert _window_label(_ist_ms(9, 46)) == "09:30"


def test_window_label_on_boundary():
    """Exactly 09:30 IST stays at 09:30."""
    assert _window_label(_ist_ms(9, 30)) == "09:30"


# ── record_stale_drop ─────────────────────────────────────────────────────────

def test_stale_drop_increments_totals():
    t = DataLossTracker()
    t.record_stale_drop("NSE_FO|NIFTY CE", _ist_ms(9, 20))
    t.record_stale_drop("NSE_FO|NIFTY PE", _ist_ms(9, 25))
    assert t._stale_total == 2
    assert t._stale_by_instrument["NSE_FO|NIFTY CE"] == 1
    assert t._stale_by_instrument["NSE_FO|NIFTY PE"] == 1
    assert t._stale_by_window["09:00"] == 2


# ── record_filter_drop ────────────────────────────────────────────────────────

def test_filter_drop_increments_totals():
    t = DataLossTracker()
    t.record_filter_drop("NSE_FO|BANKNIFTY CE", _ist_ms(9, 45))
    t.record_filter_drop("NSE_FO|BANKNIFTY CE", _ist_ms(10, 5))
    assert t._filter_total == 2
    assert t._filter_by_instrument["NSE_FO|BANKNIFTY CE"] == 2
    assert t._filter_by_window["09:30"] == 1
    assert t._filter_by_window["10:00"] == 1


# ── record_failed_chunk ───────────────────────────────────────────────────────

def test_failed_chunk_stored():
    t = DataLossTracker()
    t.record_failed_chunk("2024-10-03", _ist_ms(9, 15), _ist_ms(9, 45))
    assert len(t._failed_chunks) == 1
    assert t._failed_chunks[0] == ("2024-10-03", _ist_ms(9, 15), _ist_ms(9, 45))


# ── print_summary ─────────────────────────────────────────────────────────────

def test_summary_zero_loss(capsys):
    DataLossTracker().print_summary()
    out = capsys.readouterr().out
    assert "DATA LOSS SUMMARY" in out
    assert "Stale Drops       : 0" in out


def test_summary_contains_all_sections(capsys):
    t = DataLossTracker()
    t.record_stale_drop("NSE_FO|NIFTY CE", _ist_ms(9, 20))
    t.record_filter_drop("NSE_FO|BANKNIFTY CE", _ist_ms(9, 45))
    t.record_failed_chunk("2024-10-03", _ist_ms(9, 15), _ist_ms(9, 45))
    t.print_summary()
    out = capsys.readouterr().out
    assert "STALE DROPS BY INSTRUMENT" in out
    assert "FILTER DROPS BY INSTRUMENT" in out
    assert "FAILED FETCH CHUNKS" in out
    assert "NSE_FO|NIFTY CE" in out


def test_top10_instruments_capped(capsys):
    t = DataLossTracker()
    # Instruments 0–4 get 2 drops each (higher count), 5–14 get 1 each.
    # This makes ranking unambiguous: top 10 = INSTR_00..04 + any 5 of INSTR_05..14.
    for i in range(5):
        t.record_stale_drop(f"NSE_FO|INSTR_{i:02d}", _ist_ms(9, 20))
        t.record_stale_drop(f"NSE_FO|INSTR_{i:02d}", _ist_ms(9, 20))
    for i in range(5, 15):
        t.record_stale_drop(f"NSE_FO|INSTR_{i:02d}", _ist_ms(9, 20))
    t.print_summary()
    out = capsys.readouterr().out
    # Exactly 10 instruments in summary (cap enforced)
    assert out.count("NSE_FO|INSTR_") == 10
    # The top-5 by count must always appear
    for i in range(5):
        assert f"NSE_FO|INSTR_{i:02d}" in out


# ── DataQueue integration ─────────────────────────────────────────────────────

def test_data_queue_records_stale_drop_non_batch():
    """DataQueue.put records stale drops via tracker in non-batch mode."""
    from Simulator import DataQueue

    tracker = DataLossTracker()
    q = DataQueue()
    # start_ts=1727927200000 (09:16:40 IST); packet at 09:15 IST is stale
    q.start(batch_size=None, start_ts=1727927200000, tracker=tracker)
    q.put({"currentTs": 1727927100000, "feeds": {"NSE_FO|NIFTY CE": {}}})

    assert tracker._stale_total == 1
    assert tracker._stale_by_instrument["NSE_FO|NIFTY CE"] == 1
    assert tracker._stale_by_window["09:00"] == 1
