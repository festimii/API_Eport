from datetime import datetime
from decimal import Decimal
from typing import Any

from db import get_conn


def _normalize_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode(errors="ignore")
    if isinstance(value, bool):
        return int(value)
    return value


def fetch_daily_stock(date_str: str | None = None) -> list[dict[str, Any]]:
    """Execute Festim_Stock_Export for a date (defaults to today) and return rows."""
    target_date = date_str or datetime.today().strftime("%Y-%m-%d")

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.Festim_Stock_Export @Date = ?", target_date)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    result = [
        {column: _normalize_value(value) for column, value in zip(columns, row)}
        for row in rows
    ]

    cursor.close()
    conn.close()

    return result
