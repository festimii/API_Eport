import csv
import io
from db import get_conn

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

    cursor.execute("""
        SELECT *
        FROM dbo.ItemMaster
        ORDER BY Internal_ID;
    """)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    result = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()

    return result


def generate_csv(data: list[dict]) -> bytes:
    """Generate CSV file as bytes."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys(), delimiter=";")
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue().encode("utf-8")
