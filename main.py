from fastapi import FastAPI, Request

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
