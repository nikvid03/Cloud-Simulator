"""
Unit tests for DBManager.
create_engine and pd.read_sql are mocked — no real DB connection is made.
Env vars are injected via pytest's monkeypatch fixture.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def db(monkeypatch):
    """DBManager with injected env vars and mocked SQLAlchemy engine."""
    monkeypatch.setenv("DB_USER", "testuser")
    monkeypatch.setenv("DB_PASS", "testpass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("TABLE_NAME", "test_table")

    with patch("DBManager.create_engine") as mock_engine:
        # Import inside fixture so os.getenv reads happen after monkeypatch
        from DBManager import DBManager
        instance = DBManager()
    return instance, mock_engine


@pytest.fixture
def db_no_table(monkeypatch):
    """DBManager with TABLE_NAME unset to test default fallback."""
    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASS", "p")
    monkeypatch.setenv("DB_HOST", "h")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "d")
    monkeypatch.delenv("TABLE_NAME", raising=False)

    with patch("DBManager.create_engine"):
        from DBManager import DBManager
        instance = DBManager()
    return instance


# ---------------------------------------------------------------------------
# __init__ configuration
# ---------------------------------------------------------------------------

def test_connection_string_format(db):
    instance, mock_engine = db
    args, kwargs = mock_engine.call_args
    conn_str = args[0]
    assert conn_str == "postgresql+psycopg2://testuser:testpass@localhost:5432/testdb"


def test_pool_pre_ping_true(db):
    _, mock_engine = db
    _, kwargs = mock_engine.call_args
    assert kwargs.get("pool_pre_ping") is True


def test_connect_args_set(db):
    _, mock_engine = db
    _, kwargs = mock_engine.call_args
    connect_args = kwargs.get("connect_args", {})
    assert connect_args.get("connect_timeout") == 10
    assert connect_args.get("options") == "-c statement_timeout=30000"


def test_table_name_from_env(db):
    instance, _ = db
    assert instance.table_name == "test_table"


def test_table_name_default(db_no_table):
    assert db_no_table.table_name == "upstox_table"


# ---------------------------------------------------------------------------
# FetchBatch
# ---------------------------------------------------------------------------

def test_fetchbatch_correct_params(db):
    instance, _ = db
    mock_df = pd.DataFrame({"col": [1, 2, 3]})

    with patch("DBManager.pd.read_sql", return_value=mock_df) as mock_sql:
        instance.FetchBatch("2025-08-06", 1000, 2000)

    _, kwargs = mock_sql.call_args
    params = kwargs.get("params") or mock_sql.call_args[0][2]
    # Handle both positional and keyword call styles
    call_kwargs = mock_sql.call_args.kwargs if mock_sql.call_args.kwargs else {}
    call_args = mock_sql.call_args.args if mock_sql.call_args.args else mock_sql.call_args[0]
    # params may be positional arg[2] or keyword
    if "params" in call_kwargs:
        actual_params = call_kwargs["params"]
    else:
        actual_params = call_args[2] if len(call_args) > 2 else None

    assert actual_params == {"tickd": "2025-08-06", "start_epoch": 1000, "end_epoch": 2000}


def test_fetchbatch_query_structure(db):
    instance, _ = db
    mock_df = pd.DataFrame()

    with patch("DBManager.pd.read_sql", return_value=mock_df) as mock_sql:
        instance.FetchBatch("2025-08-06", 1000, 2000)

    sql_arg = mock_sql.call_args[0][0]
    assert "WHERE tickd = %(tickd)s" in sql_arg
    assert "AND ts >= %(start_epoch)s" in sql_arg
    assert "AND ts <= %(end_epoch)s" in sql_arg
    assert "ORDER BY ts" in sql_arg


def test_fetchbatch_returns_dataframe(db):
    instance, _ = db
    expected = pd.DataFrame({"col": [1, 2, 3]})

    with patch("DBManager.pd.read_sql", return_value=expected):
        result = instance.FetchBatch("2025-08-06", 0, 9999)

    assert len(result) == 3


# ---------------------------------------------------------------------------
# FetchDay
# ---------------------------------------------------------------------------

def test_fetchday_correct_params(db):
    instance, _ = db
    mock_df = pd.DataFrame()

    with patch("DBManager.pd.read_sql", return_value=mock_df) as mock_sql:
        instance.FetchDay("2025-08-06")

    call_kwargs = mock_sql.call_args.kwargs if mock_sql.call_args.kwargs else {}
    call_args_pos = mock_sql.call_args.args if mock_sql.call_args.args else mock_sql.call_args[0]
    if "params" in call_kwargs:
        actual_params = call_kwargs["params"]
    else:
        actual_params = call_args_pos[2] if len(call_args_pos) > 2 else None

    assert actual_params == {"tickd": "2025-08-06"}


def test_fetchday_query_structure(db):
    instance, _ = db
    mock_df = pd.DataFrame()

    with patch("DBManager.pd.read_sql", return_value=mock_df) as mock_sql:
        instance.FetchDay("2025-08-06")

    sql_arg = mock_sql.call_args[0][0]
    assert "WHERE tickd = %(tickd)s" in sql_arg
    assert "ORDER BY ts" in sql_arg
