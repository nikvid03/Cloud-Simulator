import os
import time
import sys
from datetime import datetime
from typing import List
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BUCKET = "mq-mastertrust-data-bucket"
PREFIX = "mastertrust_data"

# Use auto_detect=true so DuckDB reads column names from the CSV header.
# Explicit columns={} reads positionally (not by name) and silently misaligns
# columns when the CSV column order differs from the dict order.

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


def _date_to_folder_suffix(date_str: str) -> str:
    """Convert YYYY-MM-DD to DD-MM-YYYY for S3 folder naming."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Expected YYYY-MM-DD format, got: {date_str!r}")
    return dt.strftime("%d-%m-%Y")


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


def fetch_day(date_str: str) -> pd.DataFrame:
    """Fetch all ticks for a trading day from S3, mapped to simulator column names."""
    conn = _make_conn()
    glob = _resolve_s3_glob(date_str, conn)
    sql = f"""
        SELECT {_SELECT}
        FROM read_csv('{glob}', auto_detect=true)
        WHERE exchange_timestamp > 0
        ORDER BY exchange_timestamp
    """
    return conn.execute(sql).df()


def fetch_batch(date_str: str, start_epoch_ms: int, end_epoch_ms: int) -> pd.DataFrame:
    """Fetch ticks in [start_epoch_ms, end_epoch_ms] (epoch-ms, matching RequestWindow).

    Divides by 1000 internally since exchange_timestamp in S3 CSVs is epoch-seconds.
    Rows with exchange_timestamp=0 (pre-market stale ticks) are excluded.
    """
    conn = _make_conn()
    glob = _resolve_s3_glob(date_str, conn)
    sql = f"""
        SELECT {_SELECT}
        FROM read_csv('{glob}', auto_detect=true)
        WHERE exchange_timestamp >= {start_epoch_ms // 1000}
          AND exchange_timestamp <= {end_epoch_ms // 1000}
        ORDER BY exchange_timestamp
    """
    return conn.execute(sql).df()


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
    elif not full_df.empty and len(batch_df) >= len(full_df):
        errors.append(
            f"batch row count ({len(batch_df)}) >= full day ({len(full_df)})"
        )

    return errors


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

    # Timing thresholds are informational — S3 latency varies by network/region.
    # Overall PASS/FAIL is driven by correctness only.
    day_ok   = day_elapsed   < 60
    batch_ok = batch_elapsed < 15
    overall  = not errors

    print(f"\n{'='*52}")
    print(f"  Results")
    print(f"{'='*52}")
    print(f"  fetch_day")
    print(f"    Rows      : {day_rows:,}")
    print(f"    Time      : {day_elapsed:.2f}s  (target <60s, informational)")
    print(f"    Throughput: {day_tput:,} rows/sec")
    print(f"    Timing    : {'OK' if day_ok else 'SLOW (network-dependent)'}")
    print()
    print(f"  fetch_batch  (09:15–10:15 IST)")
    print(f"    Rows      : {batch_rows:,}")
    print(f"    Time      : {batch_elapsed:.2f}s  (target <15s, informational)")
    print(f"    Throughput: {batch_tput:,} rows/sec")
    print(f"    Timing    : {'OK' if batch_ok else 'SLOW (network-dependent)'}")
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
