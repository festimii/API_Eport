from datetime import datetime
from decimal import Decimal
from typing import Any

from db import get_conn2


PROCEDURE_INCOME = "dbo.Api_GetPranimInvoiceLines_ForApi"
PROCEDURE_RETURNS = "dbo.Api_GetKthimiInvoiceLines_Pending"


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode(errors="ignore")
    return value


def _fetch_rows(procedure_name: str) -> list[dict[str, Any]]:
    conn = get_conn2()
    cursor = conn.cursor()

    cursor.execute(f"EXEC {procedure_name};")
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, (_json_safe(value) for value in row))) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return rows


def fetch_income_lines() -> list[dict[str, Any]]:
    return _fetch_rows(PROCEDURE_INCOME)


def fetch_return_lines() -> list[dict[str, Any]]:
    return _fetch_rows(PROCEDURE_RETURNS)
