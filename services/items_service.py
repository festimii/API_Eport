from db import get_conn
from fastapi.responses import StreamingResponse
import json
from datetime import datetime
from decimal import Decimal


# ----------------------------------------------------------
# FIX: JSON safe serializer for datetime, Decimal, bytes, etc.
# ----------------------------------------------------------
def json_safe(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode(errors="ignore")
    return value


def sync_items():
    """Run merge stored procedure."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("EXEC dbo.Sync_ItemMaster;")
    conn.commit()
    cursor.close()
    conn.close()


def get_all_items():
    """Return all items as a list of dicts (legacy function)."""
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM dbo.ItemMaster
        ORDER BY Internal_ID;
    """)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    data = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()
    return data


def get_item_by_id(internal_id: str):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM dbo.ItemMaster
        WHERE Internal_ID = ?;
    """, internal_id)

    row = cursor.fetchone()

    if not row:
        return None

    columns = [col[0] for col in cursor.description]
    data = dict(zip(columns, row))

    cursor.close()
    conn.close()
    return data


# ==========================================================
# STREAM ALL ITEMS WITHOUT LOADING INTO MEMORY
# ==========================================================
def stream_all_items(since: str | None = None):
    conn = get_conn()
    cursor = conn.cursor()

    # Base query
    base_sql = "SELECT * FROM dbo.ItemMaster"

    # Add delta filtering
    if since:
        base_sql += " WHERE ModifiedDate >= ? ORDER BY Internal_ID"
        cursor.execute(base_sql, since)
    else:
        base_sql += " ORDER BY Internal_ID"
        cursor.execute(base_sql)

    columns = [c[0] for c in cursor.description]

    def row_generator():
        yield "["
        first = True

        for row in cursor:
            if not first:
                yield ","
            first = False

            item = dict(zip(columns, row))
            safe_item = {k: json_safe(v) for k, v in item.items()}
            yield json.dumps(safe_item, ensure_ascii=False)

        yield "]"

        cursor.close()
        conn.close()

    return StreamingResponse(row_generator(), media_type="application/json")
