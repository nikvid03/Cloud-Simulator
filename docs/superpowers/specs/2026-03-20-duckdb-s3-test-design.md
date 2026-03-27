# DuckDB S3 Test — Design Spec
**Date:** 2026-03-20
**Status:** Approved
**Goal:** Validate that DuckDB can replace RDS (PostgreSQL) as the data source for the Cloud Simulator, using CSV tick data stored in S3. If successful, 1.7TB of data can be migrated from RDS to S3, eliminating RDS costs.

---

## Context

The Cloud Simulator currently reads tick data from an RDS PostgreSQL table (`datafeedschema.master_table`) via `DBManager.py`. The same tick data exists in S3 as per-instrument CSV files, one file per options contract per trading day.

S3 structure:
```
s3://mq-mastertrust-data-bucket/mastertrust_data/
  stocks_data_DD-MM-YYYY/          # one folder per trading day
    BANKNIFTY 06 NOV24 52000.0 PE_46144_data.csv
    BANKNIFTY 06 NOV24 52000.0 CE_46143_data.csv
    ...                            # 250+ CSVs per day
```

The test is **read-only** and **isolated** — it does not touch the existing simulator or RDS.

---

## Deliverable

A single standalone script: `test_s3_duckdb.py`

---

## Script Design

### 1. Setup
- Load AWS credentials from `.env` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`)
- Connect DuckDB in-memory
- Install and load `httpfs` extension
- Configure S3 credentials in DuckDB

### 2. Date → S3 Path Conversion
- Input: `YYYY-MM-DD` (simulator format)
- Convert to `DD-MM-YYYY` for S3 folder name
- Try `stocks_data_DD-MM-YYYY/` first, fall back to `stock_data_DD-MM-YYYY/` (handles naming inconsistency in bucket)
- If neither path exists: raise `ValueError: No S3 folder found for date YYYY-MM-DD — is it a trading day?`

### 3. FetchDay Query
Query all CSVs for a date using a DuckDB glob with **explicit column types** (not `read_csv_auto`) to avoid schema inference failures across 250+ files:

```sql
SELECT
    symbol                              AS instrument,
    CAST(exchange_timestamp AS BIGINT) * 1000  AS ts_ms,
    last_traded_price                   AS ltp,
    last_traded_quantity                AS ltq,
    close_price                         AS cp,
    currentOpenInterest                 AS oi,
    average_trade_price                 AS atp,
    total_buy_quantity                  AS total_buy_qty,
    total_sell_quantity                 AS total_sell_qty,
    last_traded_time                    AS ltt,
    initialOpenInterest                 AS poi,
    best_bid_price,
    best_ask_price,
    best_bid_quantity,
    best_ask_quantity
FROM read_csv('s3://.../stocks_data_DD-MM-YYYY/*.csv',
    columns={
        'symbol': 'VARCHAR',
        'exchange_timestamp': 'BIGINT',
        'last_traded_price': 'DOUBLE',
        'last_traded_quantity': 'BIGINT',
        'close_price': 'DOUBLE',
        'currentOpenInterest': 'BIGINT',
        'average_trade_price': 'DOUBLE',
        'total_buy_quantity': 'BIGINT',
        'total_sell_quantity': 'BIGINT',
        'last_traded_time': 'BIGINT',
        'initialOpenInterest': 'BIGINT',
        'best_bid_price': 'DOUBLE',
        'best_ask_price': 'DOUBLE',
        'best_bid_quantity': 'BIGINT',
        'best_ask_quantity': 'BIGINT'
    }
)
ORDER BY exchange_timestamp
```

Returns a pandas DataFrame via `.df()`.

### 4. FetchBatch Query
Same as FetchDay with an added filter. `start_epoch` and `end_epoch` are accepted in **epoch milliseconds** (matching `RequestWindow` in `Simulator.py`) and divided by 1000 inside the query to compare against `exchange_timestamp` (which is epoch seconds):

```sql
WHERE exchange_timestamp >= start_epoch_ms / 1000
  AND exchange_timestamp <= end_epoch_ms / 1000
```

Test window: 09:15–10:15 IST on the test date, constructed explicitly using `ZoneInfo("Asia/Kolkata")` so the test is timezone-safe across machines.

### 5. Correctness Checks (6 checks)
1. Output is a pandas DataFrame
2. `instrument` column is non-null for all rows
3. DataFrame is sorted ascending by `ts_ms`
4. Row count > 0
5. No rows where `ts_ms` is null or zero
6. `FetchBatch` row count < `FetchDay` row count

Note: `ltp = 0` is **not** checked — deep out-of-the-money illiquid contracts legitimately report zero LTP. Null LTP is checked instead.

### 6. Benchmark
Both queries timed with `time.perf_counter()`. Results printed as:

```
=== DuckDB S3 Test Results ===
Date tested   : 2024-10-03
Bucket folder : stocks_data_03-10-2024
Expected rows : ~1,000,000–2,000,000 (250 instruments × sub-second ticks)

FetchDay
  Rows fetched : 1,234,567
  Time         : 12.34s
  Throughput   : 100,045 rows/sec
  Status       : PASS

FetchBatch (09:15–10:15 IST)
  Rows fetched : 123,456
  Time         : 3.21s
  Throughput   : 38,459 rows/sec
  Status       : PASS

Overall: PASS
```

---

## Column Mapping

| CSV column                  | Simulator column  | Notes                          |
|-----------------------------|-------------------|--------------------------------|
| `symbol`                    | `instrument`      |                                |
| `exchange_timestamp * 1000` | `ts_ms`           | CSV is epoch-seconds; output is epoch-ms |
| `last_traded_price`         | `ltp`             |                                |
| `last_traded_quantity`      | `ltq`             |                                |
| `close_price`               | `cp`              |                                |
| `currentOpenInterest`       | `oi`              |                                |
| `average_trade_price`       | `atp`             |                                |
| `total_buy_quantity`        | `total_buy_qty`   |                                |
| `total_sell_quantity`       | `total_sell_qty`  |                                |
| `last_traded_time`          | `ltt`             |                                |
| `initialOpenInterest`       | `poi`             |                                |

Columns not in RDS schema (kept as-is): `best_bid_price`, `best_ask_price`, `best_bid_quantity`, `best_ask_quantity`, `high_price`, `low_price`, `open_price`, `trade_volume`.

---

## Known Gaps vs. `__convert_to_upstox`

These columns are read by `Simulator.py:__convert_to_upstox` but are absent from S3 CSV data. They are out of scope for this test but must be resolved before building the full `S3Manager`:

| Column      | Used in simulator | S3 CSV | Agreed handling |
|-------------|-------------------|--------|-----------------|
| `ts`        | `data["ts"]` (line 229) — simulator variable named `ts_ms`, reads key `"ts"` | absent | The test script outputs `ts_ms`; future `S3Manager` must reconcile this key name with the simulator |
| `up`        | `data["up"]` (underlying price, line 259) | absent | Will be `None` — same as RDS where this appears to be unpopulated |
| `indexLtp`  | `data["indexLtp"]` (line 235, index path only) | absent | Index instruments are not present in the S3 CSVs (options only); handled separately |
| `iv`, `delta`, `theta`, `gamma`, `vega` | read in options path | absent | Already NULL in RDS; not a regression |

---

## Dependencies

```
duckdb
python-dotenv
pandas
```

---

## Success Criteria

| Criterion | Pass condition |
|---|---|
| S3 connectivity | DuckDB connects and reads without auth errors |
| Correctness | All 6 correctness checks pass |
| FetchDay performance | < 60s for a full trading day (~250 CSVs, ~1–2M rows) |
| FetchBatch performance | < 15s for a 1-hour window |

If all pass → proceed to build `S3Manager` as a drop-in replacement for `DBManager`.
If performance fails → evaluate Parquet conversion on S3 as next step.
