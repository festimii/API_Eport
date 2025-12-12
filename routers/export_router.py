from fastapi import APIRouter, Response
from services.export_service import (
    run_sync_procedure,
    fetch_items,
    generate_csv,
)

router = APIRouter(prefix="/export", tags=["Export"])


@router.get("/sync")
def sync_items():
    """Run the SQL merge procedure."""
    run_sync_procedure()
    return {"status": "OK", "message": "ItemMaster synchronized successfully"}


@router.get("/json")
def export_json():
    """Export ItemMaster as JSON."""
    data = fetch_items()
    return data


@router.get("/csv")
def export_csv():
    """Export ItemMaster as CSV."""
    data = fetch_items()

    if not data:
        return Response(content="", media_type="text/plain")

    csv_bytes = generate_csv(data)

    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=ItemMaster.csv"
        },
    )
