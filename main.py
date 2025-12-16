from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import ALLOWED_IPS, ALLOWED_IP_PREFIXES
from routers.auth_router import router as auth_router
from routers.export_router import router as export_router
from routers.items_router import router as items_router
from routers.sales_router import router as sales_router
from routers.stock_router import router as stock_router
from services.sales_service import (
    start_sales_outbox_scheduler,
    stop_sales_outbox_scheduler,
)

app = FastAPI(
    title="ItemMaster API",
    version="1.0",
)

# -------------------------
# Routers
# -------------------------

app.include_router(auth_router)
app.include_router(items_router)
app.include_router(export_router)
app.include_router(sales_router)
app.include_router(stock_router)

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

    client_ip = (
        forwarded_for.split(",")[0].strip()
        if forwarded_for
        else request.client.host if request.client else None
    )

    if not client_ip:
        return JSONResponse(status_code=403, content={"detail": "Access denied"})

    if (
        client_ip in ALLOWED_IPS
        or any(client_ip.startswith(prefix) for prefix in ALLOWED_IP_PREFIXES)
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

    return response

# -------------------------
# Health
# -------------------------

@app.get("/")
def root():
    return {"status": "running"}
