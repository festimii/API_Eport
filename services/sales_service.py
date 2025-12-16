from typing import Any

from db import get_conn


def push_sales() -> dict[str, Any]:
    """Execute Api_Push_Sales to populate today's sales outbox entries."""

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("EXEC dbo.Api_Push_Sales")
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
