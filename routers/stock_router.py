from datetime import datetime

from fastapi import APIRouter, Depends, Query

from services.stock_service import fetch_daily_stock
from .auth_router import require_token

router = APIRouter(
    prefix="/stock",
    tags=["Stock"],
    dependencies=[Depends(require_token)],
)


@router.get("/daily")
def daily_stock(date: str | None = Query(
    default=None,
    description="Date for the stock export in YYYY-MM-DD format. Defaults to today."
)):
    """Return the Festim_Stock_Export result for the provided date or today's date."""
    if date is None:
        date = datetime.today().strftime("%Y-%m-%d")

    return fetch_daily_stock(date)
