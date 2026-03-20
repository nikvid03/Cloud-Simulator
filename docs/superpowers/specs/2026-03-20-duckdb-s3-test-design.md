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

### 3. FetchDay Test
Query all CSVs for a date using DuckDB glob:
```sql
SELECT
    symbol                              AS instrument,
    exchange_timestamp * 1000           AS ts_ms,
    last_traded_price                   AS ltp,
    last_traded_quantity                AS ltq,
    close_price                         AS cp,
    currentOpenInterest                 AS oi,
    average_trade_price                 AS atp,
    total_buy_quantity                  AS total_buy_qty,
    total_sell_quantity                 AS total_sell_qty,
    last_traded_time                    AS ltt,
    initialOpenInterest                 AS poi,
    best_bid_price, best_ask_price,
    best_bid_quantity, best_ask_quantity
FROM read_csv_auto('s3://.../stocks_data_DD-MM-YYYY/*.csv')
ORDER BY exchange_timestamp
```

### 4. FetchBatch Test
Same query with an additional `WHERE exchange_timestamp BETWEEN start AND end` clause. Test window: first 1 hour of the trading day (09:15–10:15 IST).

### 5. Correctness Checks
- Output is a pandas DataFrame (same type `DBManager` returns)
- `instrument` column is non-null for all rows
- DataFrame is sorted ascending by `ts_ms`
- Row count > 0
- No rows with `ts_ms = 0` or `ltp = 0` for the full-day fetch (sanity check)
- `FetchBatch` row count < `FetchDay` row count

### 6. Benchmark
Both queries are timed using `time.perf_counter()`. Results printed as:

```
=== DuckDB S3 Test Results ===
Date tested   : 2024-10-03
Bucket folder : stocks_data_03-10-2024

FetchDay
  Rows fetched : 1,234,567
  Time         : 12.34s
  Throughput   : 100,045 rows/sec
  Status       : PASS

FetchBatch (09:15–10:15)
  Rows fetched : 123,456
  Time         : 3.21s
  Throughput   : 38,459 rows/sec
  Status       : PASS

Overall: PASS
```

---

## Column Mapping

| CSV column             | Simulator column  |
|------------------------|-------------------|
| `symbol`               | `instrument`      |
| `exchange_timestamp * 1000` | `ts_ms`      |
| `last_traded_price`    | `ltp`             |
| `last_traded_quantity` | `ltq`             |
| `close_price`          | `cp`              |
| `currentOpenInterest`  | `oi`              |
| `average_trade_price`  | `atp`             |
| `total_buy_quantity`   | `total_buy_qty`   |
| `total_sell_quantity`  | `total_sell_qty`  |
| `last_traded_time`     | `ltt`             |
| `initialOpenInterest`  | `poi`             |

Columns not in RDS schema (kept as-is): `best_bid_price`, `best_ask_price`, `best_bid_quantity`, `best_ask_quantity`, `high_price`, `low_price`, `open_price`, `trade_volume`.

Columns not in CSV (absent from S3 data): `iv`, `delta`, `theta`, `gamma`, `vega` — already confirmed NULL in RDS too, so no regression.

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
| Correctness | All 5 correctness checks pass |
| FetchDay performance | < 60s for a full trading day (~250 CSVs) |
| FetchBatch performance | < 15s for a 1-hour window |

If all pass → proceed to build `S3Manager` as a drop-in replacement for `DBManager`.
If performance fails → evaluate Parquet conversion on S3 as next step.
