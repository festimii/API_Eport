from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from services.sales_service import (
    push_sales,
    mark_sale_delivered,
    mark_bill_delivered,
    mark_sale_failed,
    list_sales,
    list_sales_grouped_by_bill,
)
from .auth_router import require_token

router = APIRouter(
    prefix="/sales",
    tags=["Sales"],
    dependencies=[Depends(require_token)],
)


class SalesDeliveryRequest(BaseModel):
    ack_id: str | None = Field(
        None,
        description="Acknowledgement identifier from upstream system",
    )
    

class SalesFailureRequest(BaseModel):
    reason: str | None = Field(
        None,
        description="Failure reason or diagnostic notes",
    )


@router.post("/push")
def push_sales_outbox():
    """
    Populate today's sales outbox using the Api_Push_Sales procedure.
    """
    return push_sales()


@router.get("/")
async def get_sales(
    status: str | None = Query(
        default=None,
        description="Filter by delivery/status flag when available.",
    ),
    since: str | None = Query(
        default=None,
        description="Return records updated on/after the given ISO datetime.",
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    group_by_bill: bool = Query(
        default=False,
        description="Group sale lines by bill identifier for aggregated delivery.",
    ),
):
    """
    Retrieve sales outbox entries with optional status and timestamp filters.
    The response includes metadata to make API-to-API consumption predictable.
    """

    try:
        return await run_in_threadpool(
            list_sales, status, since, limit, offset, group_by_bill
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/grouped")
async def get_sales_grouped(
    status: str | None = Query(
        default=None,
        description="Filter by delivery/status flag when available.",
    ),
    since: str | None = Query(
        default=None,
        description="Return records updated on/after the given ISO datetime.",
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """
    Retrieve sales outbox entries grouped by bill identifier.
    """

    try:
        return await run_in_threadpool(
            list_sales_grouped_by_bill, status, since, limit, offset
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{sale_uid}/delivered")
def mark_delivered(sale_uid: str, payload: SalesDeliveryRequest):
    """
    Mark a sale as delivered.
    """
    return mark_sale_delivered(sale_uid, payload.ack_id)


@router.post("/bill/{bill_id}/delivered")
def mark_bill_as_delivered(bill_id: str, payload: SalesDeliveryRequest):
    """
    Mark all sales linked to a bill as delivered.
    """

    try:
        return mark_bill_delivered(bill_id, payload.ack_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{sale_uid}/failed")
def mark_failed(sale_uid: str, payload: SalesFailureRequest):
    """
    Mark a sale as failed.
    """
    return mark_sale_failed(sale_uid, payload.reason)
