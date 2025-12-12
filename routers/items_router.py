from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from services.items_service import (
    sync_items,
    get_all_items,
    get_item_by_id,
    stream_all_items
)

router = APIRouter(prefix="/items", tags=["Items API"])


# ----------------------------------------------------------
# THREADSAFE SYNC OPERATION
# ----------------------------------------------------------
@router.post("/sync")
async def sync():
    """Run stored procedure asynchronously in a thread."""
    await run_in_threadpool(sync_items)
    return {"status": "OK", "message": "ItemMaster synchronized"}


# ----------------------------------------------------------
# STREAMING ENDPOINT WITH MULTITHREAD SUPPORT + DELTA-SYNC
# ----------------------------------------------------------
@router.get("/stream")
async def stream_items(
    since: str | None = Query(
        default=None,
        description="Filter by ModifiedDate. If not provided, today's date is used."
    )
):
    # ------------------------------------------------------
    # If no date provided → use today's date at 00:00:00
    # ------------------------------------------------------
    if since is None:
        today = datetime.now().strftime("%Y-%m-%d 00:00:00")
        since = today

    return await run_in_threadpool(stream_all_items, since)


# ----------------------------------------------------------
# NORMAL ENDPOINTS (JSON RETURN)
# ----------------------------------------------------------
@router.get("/")
async def all_items():
    """Return full table (blocking)."""
    return await run_in_threadpool(get_all_items)


@router.get("/{internal_id}")
async def item_detail(internal_id: str):
    item = await run_in_threadpool(get_item_by_id, internal_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
