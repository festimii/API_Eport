from fastapi import FastAPI
from routers.items_router import router as items_router

app = FastAPI(
    title="ItemMaster API",
    version="1.0"
)

app.include_router(items_router)


@app.get("/")
def root():
    return {"status": "running"}
