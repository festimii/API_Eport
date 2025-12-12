from fastapi import FastAPI
from routers.auth_router import router as auth_router
from routers.export_router import router as export_router
from routers.items_router import router as items_router

app = FastAPI(
    title="ItemMaster API",
    version="1.0"
)

app.include_router(auth_router)
app.include_router(items_router)
app.include_router(export_router)


@app.get("/")
def root():
    return {"status": "running"}
