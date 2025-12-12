import csv
import io
from datetime import datetime
from decimal import Decimal
from typing import Any

from db import get_conn


# The ItemMaster export must follow a strict, predictable column order so downstream
# systems always receive the same schema regardless of database changes.
COLUMN_ORDER = [
    "Barcode",
    "Is_PLU",
    "Weighted",
    "Internal_ID",
    "ItemStatus",
    "Item_Name",
    "Supplier_ID",
    "Supplier_Name",
    "Department_ID",
    "Department_Name",
    "SubDepartment_ID",
    "SubDepartment_Name",
    "SubCategory_Code",
    "SubCategory",
    "Model_ID",
    "Model_Name",
    "Brand_ID",
    "Brand_Name",
    "Conversion",
    "Iteam_Setup_Date",
    "IsModified",
    "ModifiedDate",
    "Min_Temp",
    "Max_Temp",
    "Box_Weight",
    "Box_X",
    "Box_Y",
    "Box_Z",
    "Box_forPallet",
    "Pallet_Base",
    "Pallet_Layer",
    "Pallet_Weight",
    "Pallet_Height",
]

def run_sync_procedure():
    """Execute the Sync_ItemMaster stored procedure."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("EXEC dbo.Sync_ItemMaster;")
    conn.commit()
    cursor.close()
    conn.close()


def fetch_items():
    """Fetch all rows from ItemMaster."""
    conn = get_conn()
    cursor = conn.cursor()

    columns_sql = ", ".join([f"[{column}]" for column in COLUMN_ORDER])
    cursor.execute(
        f"""
        SELECT {columns_sql}
        FROM dbo.ItemMaster
        ORDER BY Internal_ID;
    """
    )

    rows = cursor.fetchall()

    result = [_normalize_row(dict(zip(COLUMN_ORDER, row))) for row in rows]

    cursor.close()
    conn.close()

    return result


def generate_csv(data: list[dict]) -> bytes:
    """Generate CSV file as bytes."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=COLUMN_ORDER, delimiter=";")
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue().encode("utf-8")


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Ensure rows follow the strict schema and serialize values cleanly."""

    def format_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, bytes):
            return value.decode(errors="ignore")
        return value

    return {column: format_value(row.get(column)) for column in COLUMN_ORDER}
