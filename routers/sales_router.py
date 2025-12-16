from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from services.sales_service import (
    mark_sale_delivered,
    mark_sale_failed,
    push_sales,
)
from .auth_router import require_token

router = APIRouter(
    prefix="/sales",
    tags=["Sales"],
    dependencies=[Depends(require_token)],
)


class SalesDeliveryRequest(BaseModel):
    ack_id: str | None = Field(None, description="Acknowledgement identifier from upstream")


class SalesFailureRequest(BaseModel):
    reason: str | None = Field(None, description="Failure reason or notes")


@router.post("/push")
def push_sales_outbox():
    """Populate today's sales outbox using the Api_Push_Sales procedure."""
    return push_sales()


@router.post("/{sale_uid}/delivered")
def mark_delivered(sale_uid: str, payload: SalesDeliveryRequest):
    """Mark a sale as delivered with an optional acknowledgement id."""
    return mark_sale_delivered(sale_uid, payload.ack_id)


@router.post("/{sale_uid}/failed")
def mark_failed(sale_uid: str, payload: SalesFailureRequest):
    """Mark a sale as failed with an optional reason."""
    return mark_sale_failed(sale_uid, payload.reason)
