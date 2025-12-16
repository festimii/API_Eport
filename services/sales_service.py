from typing import Any
from threading import Event, Thread
import time
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
