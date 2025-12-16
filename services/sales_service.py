from datetime import datetime
from decimal import Decimal
from threading import Event, Thread
from typing import Any
import logging

from db import get_conn

logger = logging.getLogger(__name__)

# =========================
# Scheduler internals
# =========================

_scheduler_thread: Thread | None = None
_scheduler_stop_event: Event | None = None


def _sales_outbox_worker(interval_seconds: int) -> None:
    """
    Background worker that periodically pushes sales to outbox.
    """
    logger.info("Sales outbox scheduler started (interval=%s)", interval_seconds)

    while not _scheduler_stop_event.is_set():
        try:
            push_sales()
        except Exception:
            logger.exception("Sales outbox scheduler execution failed")

        _scheduler_stop_event.wait(interval_seconds)

    logger.info("Sales outbox scheduler stopped")


def start_sales_outbox_scheduler(interval_seconds: int = 60) -> None:
    """
    Start background scheduler that pushes sales periodically.
    Idempotent: safe to call multiple times.
    """
    global _scheduler_thread, _scheduler_stop_event

    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.warning("Sales outbox scheduler already running")
        return

    _scheduler_stop_event = Event()
    _scheduler_thread = Thread(
        target=_sales_outbox_worker,
        args=(interval_seconds,),
        daemon=True,
        name="SalesOutboxScheduler",
    )
    _scheduler_thread.start()


def stop_sales_outbox_scheduler() -> None:
    """
    Stop background scheduler gracefully.
    """
    global _scheduler_thread, _scheduler_stop_event

    if not _scheduler_thread:
        return

    _scheduler_stop_event.set()
    _scheduler_thread.join(timeout=10)

    _scheduler_thread = None
    _scheduler_stop_event = None


# =========================
# Serialization helpers
# =========================

def _json_safe(value: Any) -> Any:
    """
    Normalize values so the response is API friendly.
    """

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode(errors="ignore")
    return value


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


# =========================
# Existing DB operations
# =========================

def push_sales() -> dict[str, Any]:
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.Api_Push_Sales")
    conn.commit()

    rows_affected = cursor.rowcount

    cursor.close()
    conn.close()

    return {"status": "OK", "rows_affected": rows_affected}


def list_sales(
    status: str | None = None,
    since: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Return a paginated view of the sales outbox with optional filtering.

    The function keeps the SQL simple (select *), but the result is normalized
    so it remains predictable for API-to-API communication even if the
    underlying schema evolves.
    """

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM dbo.SalesOutbox;")
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    status_field = None
    timestamp_field = None

    if rows:
        for candidate in (
            "DeliveryStatus",
            "Status",
            "StatusFlag",
            "Flag",
            "State",
        ):
            if candidate in rows[0]:
                status_field = candidate
                break

        for candidate in (
            "UpdatedAt",
            "Updated_On",
            "ModifiedDate",
            "Modified",
            "CreatedAt",
        ):
            if candidate in rows[0]:
                timestamp_field = candidate
                break

    # Filter in Python to avoid coupling to optional database columns
    if status and status_field:
        rows = [
            row
            for row in rows
            if str(row.get(status_field) or "").lower() == status.lower()
        ]

    if since:
        if not timestamp_field:
            raise ValueError("Timestamp field not found; cannot filter by 'since'")

        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError as exc:
            raise ValueError("since must be an ISO 8601 date/time string") from exc

        filtered_rows = []
        for row in rows:
            candidate_ts = _parse_datetime(row.get(timestamp_field))
            if candidate_ts and candidate_ts >= since_dt:
                filtered_rows.append(row)
        rows = filtered_rows

    total_count = len(rows)
    paginated_rows = rows[offset : offset + limit]

    safe_rows = [
        {key: _json_safe(value) for key, value in row.items()}
        for row in paginated_rows
    ]

    status_counts: dict[str, int] | None = None
    if status_field:
        status_counts = {}
        for row in rows:
            key = str(row.get(status_field) or "").lower() or "unknown"
            status_counts[key] = status_counts.get(key, 0) + 1

    return {
        "status": "OK",
        "metadata": {
            "limit": limit,
            "offset": offset,
            "returned": len(safe_rows),
            "total": total_count,
            "status_field": status_field,
            "timestamp_field": timestamp_field,
            "status_counts": status_counts,
        },
        "data": safe_rows,
    }


def mark_sale_delivered(sale_uid: str, ack_id: str | None) -> dict[str, Any]:
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "EXEC dbo.Api_Mark_Sale_Delivered @Sale_UID = ?, @Ack_Id = ?",
        sale_uid,
        ack_id,
    )
    conn.commit()

    rows_affected = cursor.rowcount

    cursor.close()
    conn.close()

    return {"status": "OK", "rows_affected": rows_affected}


def mark_sale_failed(sale_uid: str, reason: str | None) -> dict[str, Any]:
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "EXEC dbo.Api_Mark_Sale_Failed @Sale_UID = ?, @Reason = ?",
        sale_uid,
        reason,
    )
    conn.commit()

    rows_affected = cursor.rowcount

    cursor.close()
    conn.close()

    return {"status": "OK", "rows_affected": rows_affected}
