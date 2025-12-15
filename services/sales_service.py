from datetime import datetime
from typing import Any

from db import get_conn


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-like datetime string into a datetime object."""
    # Accept both full ISO strings and date-only strings by normalizing input
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.strptime(value, ISO_FORMAT)


def push_sales(branch: int, from_date: str, to_date: str) -> dict[str, Any]:
    """Execute Api_Push_Sales to populate the outbox for a branch and date range."""
    from_dt = _parse_datetime(from_date)
    to_dt = _parse_datetime(to_date)

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.Api_Push_Sales @Branch = ?, @FromDate = ?, @ToDate = ?", branch, from_dt, to_dt)
    conn.commit()

    # Return number of rows inserted for visibility
    rows_affected = cursor.rowcount

    cursor.close()
    conn.close()

    return {"status": "OK", "rows_affected": rows_affected}


def mark_sale_delivered(sale_uid: str, ack_id: str | None) -> dict[str, Any]:
    """Mark a sale as delivered and optionally store an acknowledgement id."""
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.Api_Mark_Sale_Delivered @Sale_UID = ?, @Ack_Id = ?", sale_uid, ack_id)
    conn.commit()

    result = {"status": "OK", "rows_affected": cursor.rowcount}

    cursor.close()
    conn.close()

    return result


def mark_sale_failed(sale_uid: str, reason: str | None) -> dict[str, Any]:
    """Mark a sale as failed for the provided reason."""
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.Api_Mark_Sale_Failed @Sale_UID = ?, @Reason = ?", sale_uid, reason)
    conn.commit()

    result = {"status": "OK", "rows_affected": cursor.rowcount}

    cursor.close()
    conn.close()

    return result
