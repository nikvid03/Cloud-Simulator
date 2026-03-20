import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from test_s3_duckdb import _date_to_folder_suffix, _resolve_s3_glob, run_correctness_checks


def test_standard_date():
    assert _date_to_folder_suffix("2024-10-03") == "03-10-2024"


def test_leading_zeros():
    assert _date_to_folder_suffix("2024-09-01") == "01-09-2024"


def test_end_of_year():
    assert _date_to_folder_suffix("2024-12-31") == "31-12-2024"


def test_invalid_format_raises():
    with pytest.raises(ValueError):
        _date_to_folder_suffix("03-10-2024")  # wrong input format


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
