import logging
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from db import get_conn


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"
SCHEDULER_INTERVAL_MINUTES = 10

logger = logging.getLogger(__name__)
_scheduler_stop = threading.Event()
_scheduler_thread: threading.Thread | None = None


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-like datetime string into a datetime object."""
    # Accept both full ISO strings and date-only strings by normalizing input
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.strptime(value, ISO_FORMAT)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _normalize_sale_row(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return {column: _normalize_value(value) for column, value in zip(columns, row)}


def push_sales(branch: int, from_date: str | datetime, to_date: str | datetime) -> dict[str, Any]:
    """Execute Api_Push_Sales to populate the outbox for a branch and date range."""
    from_dt = _parse_datetime(from_date) if isinstance(from_date, str) else from_date
    to_dt = _parse_datetime(to_date) if isinstance(to_date, str) else to_date

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.Api_Push_Sales @Branch = ?, @FromDate = ?, @ToDate = ?", branch, from_dt, to_dt)
    conn.commit()

    # Return number of rows inserted for visibility
    rows_affected = cursor.rowcount

    cursor.close()
    conn.close()

    return {"status": "OK", "rows_affected": rows_affected}


def fetch_new_sales(limit: int | None = None) -> list[dict[str, Any]]:
    """Return pending sales from the outbox and mark them as fetched."""
    conn = get_conn()
    cursor = conn.cursor()

    top_clause = f"TOP ({limit}) " if limit else ""
    cursor.execute(
        f"""
        SELECT {top_clause}
            Sale_UID,
            Branch,
            POS_Group,
            Register_No,
            Receipt_No,
            Item_ID,
            Quantity,
            Price,
            Discount,
            Sale_DateTime,
            Status,
            Ack_Id,
            Created_At,
            Updated_At
        FROM dbo.Api_Sales_Outbox
        WHERE Status = 'NEW'
        ORDER BY Sale_DateTime ASC, Sale_UID ASC, Item_ID ASC;
        """
    )

    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    sales = [_normalize_sale_row(columns, row) for row in rows]

    if sales:
        cursor.executemany(
            """
            UPDATE dbo.Api_Sales_Outbox
            SET Status = 'FETCHED', Updated_At = SYSUTCDATETIME()
            WHERE Sale_UID = ? AND Item_ID = ? AND Status = 'NEW';
            """,
            [(sale["Sale_UID"], sale["Item_ID"]) for sale in sales],
        )
        conn.commit()

    cursor.close()
    conn.close()

    return sales


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


def _find_active_branches(start: datetime, end: datetime) -> list[int]:
    """Discover branches with activity in the provided window."""
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT DISTINCT Sifra_Oe
        FROM dbo.Promet
        WHERE DatumVreme >= ? AND DatumVreme < ?;
        """,
        start,
        end,
    )

    branches = [int(row[0]) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return branches


def _sales_outbox_worker(interval_minutes: int) -> None:
    while not _scheduler_stop.is_set():
        window_end = datetime.utcnow()
        window_start = window_end - timedelta(minutes=interval_minutes)

        try:
            branches = _find_active_branches(window_start, window_end)
            for branch in branches:
                push_sales(branch, window_start, window_end)
        except Exception:
            logger.exception("Sales outbox scheduler encountered an error")

        _scheduler_stop.wait(interval_minutes * 60)


def start_sales_outbox_scheduler(interval_minutes: int = SCHEDULER_INTERVAL_MINUTES) -> None:
    """Start a background thread that keeps the outbox populated."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return

    _scheduler_stop.clear()
    _scheduler_thread = threading.Thread(
        target=_sales_outbox_worker,
        args=(interval_minutes,),
        daemon=True,
        name="sales-outbox-scheduler",
    )
    _scheduler_thread.start()


def stop_sales_outbox_scheduler() -> None:
    """Signal the scheduler thread to stop."""
    if not _scheduler_thread:
        return
    _scheduler_stop.set()
