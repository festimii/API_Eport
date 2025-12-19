from fastapi import APIRouter, Depends

from services.transfers_service import fetch_income_lines, fetch_return_lines
from .auth_router import require_token

router = APIRouter(
    tags=["Transfers"],
    dependencies=[Depends(require_token)],
)


@router.get("/income")
def income():
    """Return pending income (pranim) invoice lines from the secondary database."""
    return fetch_income_lines()


@router.get("/returns")
def returns():
    """Return pending return (kthim) invoice lines from the secondary database."""
    return fetch_return_lines()
