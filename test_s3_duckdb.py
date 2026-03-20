from datetime import datetime


def _date_to_folder_suffix(date_str: str) -> str:
    """Convert YYYY-MM-DD to DD-MM-YYYY for S3 folder naming."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Expected YYYY-MM-DD format, got: {date_str!r}")
    return dt.strftime("%d-%m-%Y")
