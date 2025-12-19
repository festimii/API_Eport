import asyncio
import sys
from datetime import datetime, timezone
from ipaddress import ip_address

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import ALLOWED_IPS, ALLOWED_IP_NETWORKS
from routers.auth_router import router as auth_router
from routers.export_router import router as export_router
from routers.items_router import router as items_router
from routers.sales_router import router as sales_router
from routers.stock_router import router as stock_router
from routers.transfers_router import router as transfers_router
from services.sales_service import (
    start_sales_outbox_scheduler,
    stop_sales_outbox_scheduler,
)

# -------------------------
# Windows event loop hardening
# -------------------------
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# -------------------------
# App
# -------------------------
app = FastAPI(
    title="AlgoRetail Push Data API",
    version="1.0.8",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# -------------------------
# Routers
# -------------------------
app.include_router(auth_router)
app.include_router(items_router)
app.include_router(export_router)
app.include_router(sales_router)
app.include_router(stock_router)
app.include_router(transfers_router)

# -------------------------
# Lifecycle
# -------------------------
@app.on_event("startup")
def on_startup() -> None:
    start_sales_outbox_scheduler(interval_seconds=60)


@app.on_event("shutdown")
def on_shutdown() -> None:
    stop_sales_outbox_scheduler()

# -------------------------
# Middleware
# -------------------------
@app.middleware("http")
async def enforce_ip_allowlist(request: Request, call_next):
    forwarded_for = request.headers.get("X-Forwarded-For")

    raw_ip = (
        forwarded_for.split(",")[0].strip()
        if forwarded_for
        else request.client.host if request.client else None
    )

    if not raw_ip:
        return JSONResponse(status_code=403, content={"detail": "Access denied"})

    try:
        client_ip = ip_address(raw_ip)
    except ValueError:
        return JSONResponse(status_code=403, content={"detail": "Access denied"})

    if (
        client_ip in ALLOWED_IPS
        or any(client_ip in net for net in ALLOWED_IP_NETWORKS)
    ):
        return await call_next(request)

    return JSONResponse(status_code=403, content={"detail": "Access denied"})


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"

    return response

# -------------------------
# Health
# -------------------------
@app.get("/", include_in_schema=False)
def health():
    return {
        "service": "AlgoRetail Push Data API",
        "status": "OK",
        "state": "RUNNING",
        "version": "1.0.8",
        "time_utc": datetime.now(timezone.utc).isoformat(),
    }
