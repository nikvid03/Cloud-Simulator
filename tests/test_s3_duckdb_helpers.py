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
