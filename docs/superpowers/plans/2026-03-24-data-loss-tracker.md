# Data Loss Tracker Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `DataLossTracker` that logs every stale drop, filter drop, and failed fetch chunk in real time, then prints a structured summary (totals + by-instrument + by-time-window) when the simulation ends.

**Architecture:** A standalone `DataLossTracker` class (new file) owns all counters and formatting logic. `Simulator.__init__` creates one instance. `DataQueue.start()` receives the tracker via a new `tracker` keyword argument and stores it; stale-drop paths call it inline. `Simulator.__fetch_loop` calls it for filter drops. `Simulator.__fetch_with_retry` calls it when all retries fail. `Simulator.__sender_loop` calls `print_summary()` when the queue drains. No threads, no I/O — just counters and `print()`.

**Tech Stack:** Python 3.11+, stdlib only (`collections.defaultdict`, `datetime`, `zoneinfo`, `logging`). No new dependencies.

---

## Prerequisite: Fix FetchBatch missing ts_ms alias

**Problem:** `Simulator.__convert_to_upstox` reads `data["ts_ms"]`, but `DBManager.FetchBatch` uses `SELECT *` which returns the physical column `ts` — not `ts_ms`. This causes a `KeyError` on every tick in the main simulation path. `FetchDay` already has the fix (`ts AS ts_ms`); `FetchBatch` needs the same.

**Files:**
- Modify: `DBManager.py:39-44`

- [ ] **Step 1: Add `ts AS ts_ms` alias to FetchBatch query**

In `DBManager.FetchBatch`, replace:
```python
query = f"""
    SELECT * FROM datafeedschema.{self.table_name}
    WHERE tickd = %(tickd)s
      AND ts >= %(start_epoch)s
      AND ts <= %(end_epoch)s
    ORDER BY ts
"""
```
With:
```python
query = f"""
    SELECT *, ts AS ts_ms FROM datafeedschema.{self.table_name}
    WHERE tickd = %(tickd)s
      AND ts >= %(start_epoch)s
      AND ts <= %(end_epoch)s
    ORDER BY ts
"""
```

- [ ] **Step 2: Commit**

```bash
git add DBManager.py
git commit -m "fix: add ts AS ts_ms alias to FetchBatch to match FetchDay and Simulator expectations"
```

---

## Chunk 1: DataLossTracker class and unit tests

### Task 1: DataLossTracker class (TDD)

**Files:**
- Create: `DataLossTracker.py`
- Create: `tests/test_data_loss_tracker.py`

- [ ] **Step 1: Write all 8 failing tests**

Create `tests/test_data_loss_tracker.py`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they all fail**

```bash
.venv/bin/python -m pytest tests/test_data_loss_tracker.py -v
```

Expected: `ModuleNotFoundError: No module named 'DataLossTracker'` (all 8 FAIL)

- [ ] **Step 3: Implement DataLossTracker.py**

Create `DataLossTracker.py`:

```python
import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")


def _window_label(ts_ms: int) -> str:
    """Round ts_ms down to the nearest 30-min boundary, return HH:MM label in IST."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=_IST)
    minute_floor = (dt.minute // 30) * 30
    return f"{dt.hour:02d}:{minute_floor:02d}"


class DataLossTracker:
    def __init__(self):
        self._stale_total = 0
        self._stale_by_instrument: dict = defaultdict(int)
        self._stale_by_window: dict = defaultdict(int)

        self._filter_total = 0
        self._filter_by_instrument: dict = defaultdict(int)
        self._filter_by_window: dict = defaultdict(int)

        self._failed_chunks: List[Tuple[str, int, int]] = []

    def record_stale_drop(self, instrument: str, ts_ms: int) -> None:
        self._stale_total += 1
        self._stale_by_instrument[instrument] += 1
        self._stale_by_window[_window_label(ts_ms)] += 1
        logger.debug("[DataLoss] STALE  %s  ts=%d", instrument, ts_ms)

    def record_filter_drop(self, instrument: str, ts_ms: int) -> None:
        self._filter_total += 1
        self._filter_by_instrument[instrument] += 1
        self._filter_by_window[_window_label(ts_ms)] += 1
        logger.debug("[DataLoss] FILTER %s  ts=%d", instrument, ts_ms)

    def record_failed_chunk(self, date: str, start_ts: int, end_ts: int) -> None:
        self._failed_chunks.append((date, start_ts, end_ts))
        logger.debug("[DataLoss] CHUNK FAILED %s %d→%d", date, start_ts, end_ts)

    def print_summary(self) -> None:
        total_ticks = self._stale_total + self._filter_total
        lines = [
            "",
            "============================================================",
            "  DATA LOSS SUMMARY",
            "============================================================",
            f"  Stale Drops       : {self._stale_total:,}",
            f"  Filter Drops      : {self._filter_total:,}",
            f"  Failed Chunks     : {len(self._failed_chunks):,}",
            "  " + "─" * 26,
            f"  Total tick loss   : {total_ticks:,}  across"
            f" {len(self._failed_chunks)} failed fetch windows",
        ]

        if self._stale_total:
            lines += [
                "",
                "------------------------------------------------------------",
                "  STALE DROPS BY INSTRUMENT (top 10)",
                "------------------------------------------------------------",
            ]
            top = sorted(self._stale_by_instrument.items(), key=lambda x: -x[1])[:10]
            for inst, count in top:
                lines.append(f"  {inst:<45} {count:,}")
            lines += ["", "  STALE DROPS BY TIME WINDOW", "  " + "-" * 30]
            for window, count in sorted(self._stale_by_window.items()):
                lines.append(f"  {window}    {count:,}")

        if self._filter_total:
            lines += [
                "",
                "------------------------------------------------------------",
                "  FILTER DROPS BY INSTRUMENT (top 10)",
                "------------------------------------------------------------",
            ]
            top = sorted(self._filter_by_instrument.items(), key=lambda x: -x[1])[:10]
            for inst, count in top:
                lines.append(f"  {inst:<45} {count:,}")
            lines += ["", "  FILTER DROPS BY TIME WINDOW", "  " + "-" * 30]
            for window, count in sorted(self._filter_by_window.items()):
                lines.append(f"  {window}    {count:,}")

        if self._failed_chunks:
            lines += [
                "",
                "------------------------------------------------------------",
                "  FAILED FETCH CHUNKS",
                "------------------------------------------------------------",
            ]
            for date, start_ts, end_ts in self._failed_chunks:
                lines.append(
                    f"  {date}  {_window_label(start_ts)} – {_window_label(end_ts)}"
                    f"   (all ticks in window lost)"
                )

        lines.append("============================================================")
        output = "\n".join(lines)
        print(output)
        logger.info(output)
```

- [ ] **Step 4: Run tests to confirm all 9 pass**

```bash
.venv/bin/python -m pytest tests/test_data_loss_tracker.py -v
```

Expected: `9 passed`

- [ ] **Step 5: Commit**

```bash
git add DataLossTracker.py tests/test_data_loss_tracker.py
git commit -m "feat: add DataLossTracker with live logging and summary report"
```

---

## Chunk 2: Integrate DataLossTracker into Simulator.py

### Task 2: Wire tracker into DataQueue and Simulator (6 call-sites)

**Files:**
- Modify: `Simulator.py`
- Create test in: `tests/test_data_loss_tracker.py` (append)

The 9 sub-changes to `Simulator.py` (2a–2i), covering 6 touch-points:

**2a — Import** (top of file, after existing imports):
```python
from DataLossTracker import DataLossTracker
```

**2b — `DataQueue.__init__`**: add `self.tracker = None` after `self.vCurrDataArray = []`

**2c — `DataQueue.start`**: add `tracker=None` param and `self.tracker = tracker` assignment:
```python
def start(self, batch_size=None, start_ts=None, tracker=None):
    self.vCurrDataArray = []
    self.vBatchSize = batch_size
    self.vNextTs = start_ts
    self.tracker = tracker
```

**2d — `DataQueue.put` stale drop, batch mode** (line ~46, the `return  # stale, drop`):
```python
if pDataPacket["currentTs"] <= (self.vNextTs - self.vBatchSize):
    if self.tracker:
        instrument = next(iter(pDataPacket["feeds"]), "UNKNOWN")
        self.tracker.record_stale_drop(instrument, pDataPacket["currentTs"])
    return  # stale, drop
```

**2e — `DataQueue.put` stale drop, non-batch mode** (line ~60, the `return  # stale, drop`):
```python
if pDataPacket["currentTs"] < self.vNextTs:
    if self.tracker:
        instrument = next(iter(pDataPacket["feeds"]), "UNKNOWN")
        self.tracker.record_stale_drop(instrument, pDataPacket["currentTs"])
    return  # stale, drop
```

**2f — `Simulator.__init__`**: add `self.vTracker = DataLossTracker()` after `self.vFetchDone = threading.Event()`

**2g — `Simulator.__fetch_loop`**, pass tracker to DataQueue, record filter drops.

Replace:
```python
self.vDataQueue.start(self.vSimConfig.uBatch, start_ts)
```
With:
```python
self.vDataQueue.start(self.vSimConfig.uBatch, start_ts, tracker=self.vTracker)
```

Replace:
```python
if self.vSimConfig.uInstruments and not df.empty:
    df = df[df["instrument"].isin(self.vSimConfig.uInstruments)]
```
With:
```python
if self.vSimConfig.uInstruments and not df.empty:
    dropped = df[~df["instrument"].isin(self.vSimConfig.uInstruments)]
    for _, row in dropped.iterrows():
        # FetchBatch returns `ts` column; FetchDay aliases it as `ts_ms`. Handle both.
        ts = int(row.get("ts_ms", row.get("ts", 0)))
        self.vTracker.record_filter_drop(str(row["instrument"]), ts)
    df = df[df["instrument"].isin(self.vSimConfig.uInstruments)]
```

**2h — `Simulator.__fetch_with_retry`**, record failed chunk. Insert `record_failed_chunk` **between** the `logger.error(...)` call and the `return pd.DataFrame()` on the next line — NOT after the return (that would be unreachable):
```python
if attempt == self.MAX_FETCH_RETRIES - 1:
    logger.error(
        "Skipping chunk %d-%d after %d failures",
        start_ts, end_ts, self.MAX_FETCH_RETRIES
    )
    self.vTracker.record_failed_chunk(date, start_ts, end_ts)  # ← add this line
    return pd.DataFrame()
```

**2i — `Simulator.__sender_loop`**, print summary on exit. After `logger.info("Fetch complete and queue drained. Sender exiting.")`, add:
```python
self.vTracker.print_summary()
```

- [ ] **Step 1: Write failing test for DataQueue stale recording**

Append to `tests/test_data_loss_tracker.py`:

```python
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
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
.venv/bin/python -m pytest tests/test_data_loss_tracker.py::test_data_queue_records_stale_drop_non_batch -v
```

Expected: FAIL (`AttributeError: 'DataQueue' object has no attribute 'tracker'` or similar)

- [ ] **Step 3: Apply all 9 sub-changes (2a–2i) to Simulator.py**

Make all changes 2a through 2i described above to `Simulator.py`. Do not stop early — 2g, 2h, and 2i are the three most critical integration points.

- [ ] **Step 4: Run all tests to confirm everything passes**

```bash
.venv/bin/python -m pytest tests/test_data_loss_tracker.py tests/test_s3_duckdb_helpers.py -v
```

Expected: `25 passed` (9 original tracker tests + 1 new DataQueue test + 15 S3 helper tests)

- [ ] **Step 5: Commit**

```bash
git add Simulator.py tests/test_data_loss_tracker.py
git commit -m "feat: wire DataLossTracker into DataQueue and Simulator for live + summary loss reporting"
```
