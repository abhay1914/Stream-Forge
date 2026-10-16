from fastapi import FastAPI

from backend.app.api.routes import router

app = FastAPI(
    title="StreamForge API",
    description="Temperature event processing API",
    version="0.1.0",
)

app.include_router(router)


@app.get("/")
def root():
    return {
        "message": "StreamForge API is running",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "healthy"}