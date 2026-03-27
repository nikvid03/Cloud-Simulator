# DuckDB S3 Test Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write a standalone script that validates DuckDB can read S3 CSV tick data and produce the same time-ordered stream the simulator expects, with a pass/fail benchmark.

**Architecture:** A single `test_s3_duckdb.py` script with three layers — a DuckDB connection helper, two query functions (`fetch_day`, `fetch_batch`), and a runner that times both, runs correctness checks, and prints a report. Unit tests cover the date conversion and correctness check logic; S3 query functions are validated by running the script end-to-end.

**Tech Stack:** `duckdb>=0.10.0` (httpfs extension), `pandas`, `python-dotenv`, `pytest` (for unit tests), `zoneinfo` (stdlib, Python 3.9+)

**Spec:** `docs/superpowers/specs/2026-03-20-duckdb-s3-test-design.md`

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `test_s3_duckdb.py` | Standalone validation script |
| Create | `tests/test_s3_duckdb_helpers.py` | Unit tests for date conversion and correctness checks |
| Create | `conftest.py` | Empty root conftest (required alongside pytest.ini pythonpath) |
| Modify | `requirements.txt` | Add `duckdb>=0.10.0` |
| Modify | `pytest.ini` | Add `pythonpath = .` so tests can import from project root |

---

## Chunk 1: Foundations

### Task 1: Add dependencies and root conftest

**Files:**
- Modify: `requirements.txt`
- Create: `conftest.py`

- [ ] **Step 1: Add duckdb to requirements.txt**

Open `requirements.txt` and add:
```
duckdb>=0.10.0
```
(`python-dotenv` is already present.)

- [ ] **Step 2: Install**

```bash
pip install "duckdb>=0.10.0"
```

Expected: installs without error.

- [ ] **Step 3: Add `pythonpath = .` to `pytest.ini` and create root conftest**

In `pytest.ini`, add `pythonpath = .` under `[pytest]`:

```ini
[pytest]
testpaths = tests
pythonpath = .
```

Then create `conftest.py` at project root (empty):

```python
# conftest.py — intentionally empty
```

Together these ensure `from test_s3_duckdb import ...` resolves correctly in all pytest versions.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt pytest.ini conftest.py
git commit -m "feat: add duckdb dependency, pytest pythonpath, and root conftest"
```

---

### Task 2: Date conversion helper (TDD)

**Files:**
- Create: `tests/test_s3_duckdb_helpers.py`
- Create: `test_s3_duckdb.py` (partial — just `_date_to_folder_suffix`)

- [ ] **Step 1: Write failing tests**

Create `tests/test_s3_duckdb_helpers.py`:

```python
import pytest
from test_s3_duckdb import _date_to_folder_suffix


def test_standard_date():
    assert _date_to_folder_suffix("2024-10-03") == "03-10-2024"


def test_leading_zeros():
    assert _date_to_folder_suffix("2024-09-01") == "01-09-2024"


def test_end_of_year():
    assert _date_to_folder_suffix("2024-12-31") == "31-12-2024"


def test_invalid_format_raises():
    with pytest.raises(ValueError):
        _date_to_folder_suffix("03-10-2024")  # wrong input format
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_s3_duckdb_helpers.py -v
```

Expected: `ModuleNotFoundError` (file doesn't exist yet).

- [ ] **Step 3: Create `test_s3_duckdb.py` with just the helper**

```python
from datetime import datetime


def _date_to_folder_suffix(date_str: str) -> str:
    """Convert YYYY-MM-DD to DD-MM-YYYY for S3 folder naming."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Expected YYYY-MM-DD format, got: {date_str!r}")
    return dt.strftime("%d-%m-%Y")
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_s3_duckdb_helpers.py -v
```

Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add test_s3_duckdb.py tests/test_s3_duckdb_helpers.py
git commit -m "feat: add date conversion helper with tests"
```

---

### Task 3: DuckDB connection setup

**Files:**
- Modify: `test_s3_duckdb.py`

- [ ] **Step 1: Add constants and connection factory**

Append below `_date_to_folder_suffix`:

```python
import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

BUCKET = "mq-mastertrust-data-bucket"
PREFIX = "mastertrust_data"

# Explicit column types — prevents schema inference failures across 250+ CSVs.
# All 23 columns are typed. 15 are projected in _SELECT. The remaining 8
# (exchange_code, instrument_token, high_price, low_price, open_price,
# trade_volume, yearly_high_price, yearly_low_price) are typed here for
# future use but intentionally omitted from SELECT to keep output lean.
_CSV_COLUMNS = {
    "symbol":               "VARCHAR",
    "exchange_code":        "INTEGER",
    "instrument_token":     "BIGINT",
    "exchange_timestamp":   "BIGINT",
    "last_traded_price":    "DOUBLE",
    "last_traded_quantity": "BIGINT",
    "close_price":          "DOUBLE",
    "currentOpenInterest":  "BIGINT",
    "average_trade_price":  "DOUBLE",
    "total_buy_quantity":   "BIGINT",
    "total_sell_quantity":  "BIGINT",
    "last_traded_time":     "BIGINT",
    "initialOpenInterest":  "BIGINT",
    "best_bid_price":       "DOUBLE",
    "best_ask_price":       "DOUBLE",
    "best_bid_quantity":    "BIGINT",
    "best_ask_quantity":    "BIGINT",
    "high_price":           "DOUBLE",
    "low_price":            "DOUBLE",
    "open_price":           "DOUBLE",
    "trade_volume":         "BIGINT",
    "yearly_high_price":    "DOUBLE",
    "yearly_low_price":     "DOUBLE",
}

# 15 columns projected to simulator schema.
# exchange_timestamp is declared BIGINT above so no CAST needed; * 1000
# converts epoch-seconds to epoch-ms.
_SELECT = """
    symbol                          AS instrument,
    exchange_timestamp * 1000       AS ts_ms,
    last_traded_price               AS ltp,
    last_traded_quantity            AS ltq,
    close_price                     AS cp,
    currentOpenInterest             AS oi,
    average_trade_price             AS atp,
    total_buy_quantity              AS total_buy_qty,
    total_sell_quantity             AS total_sell_qty,
    last_traded_time                AS ltt,
    initialOpenInterest             AS poi,
    best_bid_price,
    best_ask_price,
    best_bid_quantity,
    best_ask_quantity
"""


def _make_conn() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection configured for S3 access.

    Requires internet access for 'INSTALL httpfs' on first run (downloads
    the extension from the DuckDB extension registry). Subsequent runs use
    the cached extension.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    # CREATE SECRET avoids f-string interpolation of credential values.
    # Requires duckdb>=0.10.0.
    conn.execute("""
        CREATE OR REPLACE SECRET aws_creds (
            TYPE S3,
            KEY_ID ?,
            SECRET ?,
            REGION ?
        )
    """, [
        os.environ["AWS_ACCESS_KEY_ID"],
        os.environ["AWS_SECRET_ACCESS_KEY"],
        os.environ["AWS_DEFAULT_REGION"],
    ])
    return conn
```

- [ ] **Step 2: Smoke-test the connection**

```bash
python - <<'EOF'
from test_s3_duckdb import _make_conn
conn = _make_conn()
print("DuckDB connected:", conn.execute("SELECT 42").fetchone())
EOF
```

Expected: `DuckDB connected: (42,)`

- [ ] **Step 3: Commit**

```bash
git add test_s3_duckdb.py
git commit -m "feat: add DuckDB S3 connection factory with CREATE SECRET"
```

---

## Chunk 2: Query Functions, Checks, and Runner

### Task 4: S3 glob resolver with fallback

**Files:**
- Modify: `test_s3_duckdb.py`
- Modify: `tests/test_s3_duckdb_helpers.py`

`_resolve_s3_glob` accepts an optional `conn` parameter so callers can reuse an existing connection rather than paying the `INSTALL httpfs` overhead twice.

- [ ] **Step 1: Write failing tests**

Add to `tests/test_s3_duckdb_helpers.py`:

```python
from unittest.mock import patch, MagicMock
from test_s3_duckdb import _resolve_s3_glob


def test_resolve_raises_on_missing_date():
    """Both folder variants return 0 files — should raise ValueError."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = (0,)
    with patch("test_s3_duckdb._make_conn", return_value=mock_conn):
        with pytest.raises(ValueError, match="No S3 folder found"):
            _resolve_s3_glob("2024-10-06")


def test_resolve_returns_stocks_variant_first():
    """stocks_data_ variant found on first try — returns without trying fallback."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = (10,)
    with patch("test_s3_duckdb._make_conn", return_value=mock_conn):
        result = _resolve_s3_glob("2024-10-03")
    assert "stocks_data_03-10-2024" in result
    assert result.endswith("*.csv")
    # Verify short-circuit: execute called exactly once for the glob check
    assert mock_conn.execute.call_count == 1
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_s3_duckdb_helpers.py -v
```

Expected: `ImportError` on `_resolve_s3_glob`.

- [ ] **Step 3: Add `_resolve_s3_glob` to `test_s3_duckdb.py`**

```python
def _resolve_s3_glob(date_str: str, conn: duckdb.DuckDBPyConnection = None) -> str:
    """Return S3 glob path for all CSVs on this date.

    Tries stocks_data_DD-MM-YYYY/ first, falls back to stock_data_DD-MM-YYYY/.
    Accepts an optional existing connection to avoid double httpfs setup overhead.

    Note: the S3 URL is built from module-level constants (BUCKET, PREFIX) and
    a validated date suffix — not user input — so f-string interpolation here
    is safe and does not represent an injection risk.
    """
    suffix = _date_to_folder_suffix(date_str)
    _conn = conn if conn is not None else _make_conn()
    for folder_prefix in ("stocks_data", "stock_data"):
        folder = f"{folder_prefix}_{suffix}"
        glob = f"s3://{BUCKET}/{PREFIX}/{folder}/*.csv"
        try:
            count = _conn.execute(
                f"SELECT COUNT(*) FROM glob('{glob}')"
            ).fetchone()[0]
            if count > 0:
                return glob
        except Exception:
            continue
    raise ValueError(
        f"No S3 folder found for date {date_str} — is it a trading day?"
    )
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_s3_duckdb_helpers.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add test_s3_duckdb.py tests/test_s3_duckdb_helpers.py
git commit -m "feat: add S3 glob resolver with folder fallback"
```

---

### Task 5: `fetch_day` and `fetch_batch` functions

**Files:**
- Modify: `test_s3_duckdb.py`

Each function creates one connection and passes it to `_resolve_s3_glob` — so `INSTALL httpfs` is paid once per call, not twice.

- [ ] **Step 1: Add both query functions**

```python
import pandas as pd


def fetch_day(date_str: str) -> pd.DataFrame:
    """Fetch all ticks for a trading day from S3, mapped to simulator column names."""
    conn = _make_conn()
    glob = _resolve_s3_glob(date_str, conn)
    # _CSV_COLUMNS is a hardcoded module-level constant — no user input reaches
    # this f-string, so the dict-as-string interpolation is safe here.
    cols_def = ", ".join(f"'{k}': '{v}'" for k, v in _CSV_COLUMNS.items())
    sql = f"""
        SELECT {_SELECT}
        FROM read_csv('{glob}', columns={{{cols_def}}})
        ORDER BY exchange_timestamp
    """
    return conn.execute(sql).df()


def fetch_batch(date_str: str, start_epoch_ms: int, end_epoch_ms: int) -> pd.DataFrame:
    """Fetch ticks in [start_epoch_ms, end_epoch_ms] (epoch-ms, matching RequestWindow).

    Divides by 1000 internally since exchange_timestamp in S3 CSVs is epoch-seconds.
    """
    conn = _make_conn()
    glob = _resolve_s3_glob(date_str, conn)
    cols_def = ", ".join(f"'{k}': '{v}'" for k, v in _CSV_COLUMNS.items())
    sql = f"""
        SELECT {_SELECT}
        FROM read_csv('{glob}', columns={{{cols_def}}})
        WHERE exchange_timestamp >= {start_epoch_ms // 1000}
          AND exchange_timestamp <= {end_epoch_ms // 1000}
        ORDER BY exchange_timestamp
    """
    return conn.execute(sql).df()
```

- [ ] **Step 2: Commit**

```bash
git add test_s3_duckdb.py
git commit -m "feat: add fetch_day and fetch_batch — single connection per call"
```

---

### Task 6: Correctness checks (TDD)

**Files:**
- Modify: `tests/test_s3_duckdb_helpers.py`
- Modify: `test_s3_duckdb.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_s3_duckdb_helpers.py`:

```python
import pandas as pd
from test_s3_duckdb import run_correctness_checks

# Full 15-column schema matching the simulator output
_ALL_COLS = [
    "instrument", "ts_ms", "ltp", "ltq", "cp", "oi", "atp",
    "total_buy_qty", "total_sell_qty", "ltt", "poi",
    "best_bid_price", "best_ask_price", "best_bid_quantity", "best_ask_quantity",
]


def _make_df(rows, cols=None):
    return pd.DataFrame(rows, columns=cols or _ALL_COLS)


def _full_row(instrument="NIFTY CE", ts_ms=1000, ltp=100.0):
    """Return a single row with all 15 columns populated."""
    return (instrument, ts_ms, ltp, 1, 99.0, 500, 100.5, 10, 8, 1000, 400,
            99.5, 100.5, 10, 8)


def test_checks_pass_on_valid_full_schema():
    """Happy path with full 15-column schema — all checks pass."""
    full = _make_df([
        _full_row("NIFTY CE", 1000),
        _full_row("NIFTY CE", 2000),
        _full_row("NIFTY PE", 3000),
    ])
    batch = _make_df([_full_row("NIFTY CE", 1000)])
    assert run_correctness_checks(full, batch) == []


def test_checks_fail_on_unsorted():
    full = _make_df([_full_row(ts_ms=3000), _full_row(ts_ms=1000)])
    batch = _make_df([_full_row(ts_ms=1000)])
    errors = run_correctness_checks(full, batch)
    assert any("sorted" in e for e in errors)


def test_checks_fail_on_null_instrument():
    full = _make_df([_full_row(instrument=None, ts_ms=1000), _full_row(ts_ms=2000)])
    batch = _make_df([_full_row(ts_ms=1000)])
    errors = run_correctness_checks(full, batch)
    assert any("instrument" in e for e in errors)


def test_checks_fail_on_empty_df():
    errors = run_correctness_checks(_make_df([]), _make_df([]))
    assert any("empty" in e.lower() for e in errors)


def test_checks_fail_when_batch_larger():
    full = _make_df([_full_row(ts_ms=1000)])
    batch = _make_df([_full_row(ts_ms=1000), _full_row(ts_ms=2000)])
    errors = run_correctness_checks(full, batch)
    assert any("batch" in e.lower() for e in errors)


def test_checks_fail_when_batch_equal_size():
    full = _make_df([_full_row(ts_ms=1000)])
    batch = _make_df([_full_row(ts_ms=1000)])
    errors = run_correctness_checks(full, batch)
    assert any("batch" in e.lower() for e in errors)


def test_checks_fail_on_zero_ts():
    full = _make_df([_full_row(ts_ms=0), _full_row(ts_ms=1000)])
    batch = _make_df([_full_row(ts_ms=1000)])
    errors = run_correctness_checks(full, batch)
    assert any("ts_ms" in e for e in errors)


def test_checks_fail_on_empty_batch_with_nonempty_full():
    """An empty batch against a non-empty full day is a failure."""
    full = _make_df([_full_row(ts_ms=1000), _full_row(ts_ms=2000)])
    batch = _make_df([])
    errors = run_correctness_checks(full, batch)
    assert any("batch" in e.lower() for e in errors)
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_s3_duckdb_helpers.py -v -k "test_checks"
```

Expected: `ImportError` on `run_correctness_checks`.

- [ ] **Step 3: Add `run_correctness_checks` to `test_s3_duckdb.py`**

```python
from typing import List


def run_correctness_checks(full_df: pd.DataFrame, batch_df: pd.DataFrame) -> List[str]:
    """Run all 6 correctness checks. Returns list of error strings (empty = all pass)."""
    errors = []

    # Check 1: output is a DataFrame
    if not isinstance(full_df, pd.DataFrame):
        errors.append("fetch_day did not return a DataFrame")
        return errors

    # Check 2: non-empty
    if full_df.empty:
        errors.append("fetch_day returned empty DataFrame")

    # Check 3: instrument column non-null
    if "instrument" in full_df.columns and full_df["instrument"].isnull().any():
        errors.append("instrument column contains null values")

    # Check 4: sorted by ts_ms ascending
    if "ts_ms" in full_df.columns and not full_df["ts_ms"].is_monotonic_increasing:
        errors.append("DataFrame is not sorted ascending by ts_ms")

    # Check 5: no zero or null ts_ms
    if "ts_ms" in full_df.columns:
        bad = full_df["ts_ms"].isnull() | (full_df["ts_ms"] == 0)
        if bad.any():
            errors.append(f"ts_ms has {bad.sum()} null/zero values")

    # Check 6: batch strictly smaller than full day.
    # Split into two cases so error messages are clear:
    # a) empty batch against a non-empty full day — always wrong
    # b) batch count >= full count — wrong regardless of empty
    if not full_df.empty and batch_df.empty:
        errors.append("batch returned 0 rows against a non-empty full day")
    elif len(batch_df) >= len(full_df):
        errors.append(
            f"batch row count ({len(batch_df)}) >= full day ({len(full_df)})"
        )

    return errors
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_s3_duckdb_helpers.py -v -k "test_checks"
```

Expected: 8 PASS.

- [ ] **Step 5: Commit**

```bash
git add test_s3_duckdb.py tests/test_s3_duckdb_helpers.py
git commit -m "feat: add correctness checks with full test coverage"
```

---

### Task 7: Benchmark runner and main entrypoint

**Files:**
- Modify: `test_s3_duckdb.py`

- [ ] **Step 1: Add the runner**

```python
import time
import sys
from zoneinfo import ZoneInfo
from datetime import datetime


def _ist_epoch_ms(date_str: str, hour: int, minute: int) -> int:
    """Return epoch-ms for a given time on date_str in IST (Asia/Kolkata)."""
    ist = ZoneInfo("Asia/Kolkata")
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=hour, minute=minute, second=0, microsecond=0, tzinfo=ist
    )
    return int(dt.timestamp() * 1000)


def run(date_str: str = "2024-10-03") -> bool:
    print(f"\n{'='*52}")
    print(f"  DuckDB S3 Test   |   Date: {date_str}")
    print(f"{'='*52}\n")

    # FetchDay
    print("Running fetch_day ...")
    try:
        t0 = time.perf_counter()
        full_df = fetch_day(date_str)
        day_elapsed = time.perf_counter() - t0
    except Exception as exc:
        print(f"  fetch_day FAILED: {exc}")
        return False

    day_rows = len(full_df)
    day_tput = int(day_rows / day_elapsed) if day_elapsed > 0 else 0

    # FetchBatch (09:15–10:15 IST)
    print("Running fetch_batch (09:15–10:15 IST) ...")
    try:
        start_ms = _ist_epoch_ms(date_str, 9, 15)
        end_ms   = _ist_epoch_ms(date_str, 10, 15)
        t0 = time.perf_counter()
        batch_df = fetch_batch(date_str, start_ms, end_ms)
        batch_elapsed = time.perf_counter() - t0
    except Exception as exc:
        print(f"  fetch_batch FAILED: {exc}")
        return False

    batch_rows = len(batch_df)
    batch_tput = int(batch_rows / batch_elapsed) if batch_elapsed > 0 else 0

    # Correctness
    errors = run_correctness_checks(full_df, batch_df)

    day_ok   = day_elapsed   < 60
    batch_ok = batch_elapsed < 15
    overall  = day_ok and batch_ok and not errors

    print(f"\n{'='*52}")
    print(f"  Results")
    print(f"{'='*52}")
    print(f"  fetch_day")
    print(f"    Rows      : {day_rows:,}")
    print(f"    Time      : {day_elapsed:.2f}s  (threshold <60s)")
    print(f"    Throughput: {day_tput:,} rows/sec")
    print(f"    Timing    : {'PASS' if day_ok else 'FAIL'}")
    print()
    print(f"  fetch_batch  (09:15–10:15 IST)")
    print(f"    Rows      : {batch_rows:,}")
    print(f"    Time      : {batch_elapsed:.2f}s  (threshold <15s)")
    print(f"    Throughput: {batch_tput:,} rows/sec")
    print(f"    Timing    : {'PASS' if batch_ok else 'FAIL'}")
    print()
    if errors:
        print("  Correctness Errors:")
        for e in errors:
            print(f"    x {e}")
    else:
        print("  Correctness  : all 6 checks PASS")
    print()
    print(f"  Overall: {'PASS' if overall else 'FAIL'}")
    print(f"{'='*52}\n")
    return overall


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-10-03"
    sys.exit(0 if run(date) else 1)
```

- [ ] **Step 2: Run all unit tests**

```bash
pytest tests/test_s3_duckdb_helpers.py -v
```

Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add test_s3_duckdb.py
git commit -m "feat: add benchmark runner and main entrypoint"
```

---

### Task 8: Integration run against real S3

- [ ] **Step 1: Run the script**

```bash
python test_s3_duckdb.py 2024-10-03
```

Expected output:
```
====================================================
  DuckDB S3 Test   |   Date: 2024-10-03
====================================================

Running fetch_day ...
Running fetch_batch (09:15–10:15 IST) ...

====================================================
  Results
====================================================
  fetch_day
    Rows      : X,XXX,XXX
    Time      : XX.XXs  (threshold <60s)
    Throughput: XXX,XXX rows/sec
    Timing    : PASS

  fetch_batch  (09:15–10:15 IST)
    Rows      : XXX,XXX
    Time      : X.XXs  (threshold <15s)
    Throughput: XXX,XXX rows/sec
    Timing    : PASS

  Correctness  : all 6 checks PASS

  Overall: PASS
====================================================
```

- [ ] **Step 2: If FAIL — diagnose**

| Symptom | Likely cause | Fix |
|---|---|---|
| Auth error / `CREATE SECRET` fails | Expired AWS key | Refresh key in `.env` |
| `ValueError: No S3 folder found` | Weekend / holiday date | Try `2024-10-03` (Thursday) |
| Correctness error on `ts_ms` | CSV has malformed rows | `aws s3 cp <file> - \| head -5` |
| `INSTALL httpfs` fails | No internet access | Run where internet is available |
| `fetch_day` > 60s | Cold S3 + large dataset | Note time; consider Parquet migration |
| `fetch_batch` 0 rows → check 6 fail | Epoch conversion bug | Print `start_ms // 1000` vs CSV `exchange_timestamp` |

- [ ] **Step 3: Fill in results and commit**

Add to `docs/superpowers/specs/2026-03-20-duckdb-s3-test-design.md`:

```markdown
## Test Results (2026-03-20)

| Metric | Result |
|--------|--------|
| Date tested | 2024-10-03 |
| FetchDay rows | _fill in_ |
| FetchDay time | _fill in_ |
| FetchBatch rows | _fill in_ |
| FetchBatch time | _fill in_ |
| Overall | PASS / FAIL |

**Decision:** _fill in after run_
```

```bash
git add docs/superpowers/specs/2026-03-20-duckdb-s3-test-design.md
git commit -m "docs: add test results placeholder to DuckDB S3 spec"
```
