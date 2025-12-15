from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import ALLOWED_IPS, ALLOWED_IP_PREFIXES
from routers.auth_router import router as auth_router
from routers.export_router import router as export_router
from routers.items_router import router as items_router
from routers.stock_router import router as stock_router

app = FastAPI(
    title="ItemMaster API",
    version="1.0"
)

app.include_router(auth_router)
app.include_router(items_router)
app.include_router(export_router)
app.include_router(stock_router)


@app.middleware("http")
async def enforce_ip_allowlist(request: Request, call_next):
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    client_ip = forwarded_for.split(",")[0].strip() if forwarded_for else None
    if not client_ip and request.client:
        client_ip = request.client.host

    if client_ip:
        if client_ip in ALLOWED_IPS or any(
            client_ip.startswith(prefix) for prefix in ALLOWED_IP_PREFIXES
        ):
            return await call_next(request)

    return JSONResponse(status_code=403, content={"detail": "Access denied"})


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-XSS-Protection", "1; mode=block")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Cache-Control", "no-store")
    return response


@app.get("/")
def root():
    return {"status": "running"}
