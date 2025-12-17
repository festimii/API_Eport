from datetime import datetime
from decimal import Decimal
from threading import Event, Thread
from typing import Any
import logging

from db import get_conn

logger = logging.getLogger(__name__)

STATUS_FIELD_CANDIDATES = (
    "DeliveryStatus",
    "Status",
    "StatusFlag",
    "Flag",
    "State",
)

TIMESTAMP_FIELD_CANDIDATES = (
    "UpdatedAt",
    "Updated_On",
    "ModifiedDate",
    "Modified",
    "CreatedAt",
)

BILL_ID_FIELD_CANDIDATES = (
    "BillId",
    "Bill_ID",
    "BillID",
    "Bill_No",
    "BillNo",
    "BillNumber",
    "InvoiceId",
    "Invoice_ID",
    "InvoiceNo",
)

SALE_UID_FIELD_CANDIDATES = (
    "Sale_UID",
    "SaleUID",
    "SaleId",
    "Sale_ID",
    "UID",
    "Id",
)

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


def _detect_field(row: dict[str, Any], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in row:
            return candidate
    return None


def _fetch_sales_rows() -> list[dict[str, Any]]:
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM dbo.Api_Sales_Outbox;")
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return rows


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
    group_by_bill: bool = False,
) -> dict[str, Any]:
    """
    Return a paginated view of the sales outbox with optional filtering.

    The function keeps the SQL simple (select *), but the result is normalized
    so it remains predictable for API-to-API communication even if the
    underlying schema evolves.
    """

    rows = _fetch_sales_rows()

    status_field = None
    timestamp_field = None

    bill_id_field = None

    if rows:
        status_field = _detect_field(rows[0], STATUS_FIELD_CANDIDATES)
        timestamp_field = _detect_field(rows[0], TIMESTAMP_FIELD_CANDIDATES)
        bill_id_field = _detect_field(rows[0], BILL_ID_FIELD_CANDIDATES)

    # Filter in Python to avoid coupling to optional database columns
    if status and status_field:
        rows = [
            row
            for row in rows
            if str(row.get(status_field) or "").lower() == status.lower()
        ]
    elif status_field:
        rows = [
            row
            for row in rows
            if str(row.get(status_field) or "").lower() != "delivered"
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

    if group_by_bill:
        if not bill_id_field:
            raise ValueError("Bill identifier field not found in sales outbox")

        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            bill_key = str(row.get(bill_id_field) or "")
            grouped.setdefault(bill_key, []).append(row)

        group_items = []
        for bill_key, group_rows in sorted(grouped.items()):
            safe_group_rows = [
                {key: _json_safe(value) for key, value in row.items()}
                for row in group_rows
            ]
            group_items.append(
                {
                    "bill_id": bill_key,
                    "count": len(group_rows),
                    "sales": safe_group_rows,
                }
            )

        total_count = len(group_items)
        paginated_rows = group_items[offset : offset + limit]
    else:
        total_count = len(rows)
        paginated_rows = rows[offset : offset + limit]

        paginated_rows = [
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
            "returned": len(paginated_rows),
            "total": total_count,
            "grouped_by_bill": group_by_bill,
            "bill_id_field": bill_id_field,
            "status_field": status_field,
            "timestamp_field": timestamp_field,
            "status_counts": status_counts,
        },
        "data": paginated_rows,
    }


def list_sales_grouped_by_bill(
    status: str | None = None,
    since: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    rows = _fetch_sales_rows()

    if not rows:
        return {
            "status": "OK",
            "metadata": {
                "limit": limit,
                "offset": offset,
                "returned": 0,
                "total": 0,
                "bill_id_field": None,
                "status_field": None,
                "timestamp_field": None,
            },
            "data": [],
        }

    status_field = _detect_field(rows[0], STATUS_FIELD_CANDIDATES)
    timestamp_field = _detect_field(rows[0], TIMESTAMP_FIELD_CANDIDATES)
    bill_id_field = _detect_field(rows[0], BILL_ID_FIELD_CANDIDATES)

    if not bill_id_field:
        raise ValueError("Bill identifier field not found in sales outbox")

    if status and status_field:
        rows = [
            row
            for row in rows
            if str(row.get(status_field) or "").lower() == status.lower()
        ]
    elif status_field:
        rows = [
            row
            for row in rows
            if str(row.get(status_field) or "").lower() != "delivered"
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

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        bill_key = str(row.get(bill_id_field) or "")
        grouped.setdefault(bill_key, []).append(row)

    group_items = []
    for bill_key, group_rows in sorted(grouped.items()):
        safe_group_rows = [
            {key: _json_safe(value) for key, value in row.items()}
            for row in group_rows
        ]
        group_items.append(
            {
                "bill_id": bill_key,
                "count": len(group_rows),
                "sales": safe_group_rows,
            }
        )

    total_groups = len(group_items)
    paginated_groups = group_items[offset : offset + limit]

    return {
        "status": "OK",
        "metadata": {
            "limit": limit,
            "offset": offset,
            "returned": len(paginated_groups),
            "total": total_groups,
            "bill_id_field": bill_id_field,
            "status_field": status_field,
            "timestamp_field": timestamp_field,
        },
        "data": paginated_groups,
    }


def mark_bills_delivered(bill_ids: list[str]) -> dict[str, Any]:
    """
    Stage bill identifiers for delivery and trigger the bulk stored procedure.

    This is the only supported delivery update path; individual sale updates
    are intentionally removed in favor of bulk processing.
    """

    if not bill_ids:
        return {
            "status": "OK",
            "rows_affected": 0,
            "bill_summaries": [],
        }

    normalized_bill_ids = sorted({str(bill_id) for bill_id in bill_ids})

    conn = get_conn()
    cursor = conn.cursor()

    cursor.executemany(
        "INSERT INTO dbo.Api_Bill_Delivery_Updates (Bill_UID) VALUES (?);",
        [(bill_uid,) for bill_uid in normalized_bill_ids],
    )

    cursor.execute("EXEC dbo.Api_Mark_Bills_Delivered_Bulk")
    conn.commit()

    rows_affected = max(cursor.rowcount, 0)

    cursor.close()
    conn.close()

    return {
        "status": "OK",
        "rows_affected": rows_affected,
        "bill_summaries": [
            {"bill_id": bill_uid, "rows_affected": rows_affected}
            for bill_uid in normalized_bill_ids
        ],
    }


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
