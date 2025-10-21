from fastapi import FastAPI, Query
from datetime import datetime
from api.routes import v1

app = FastAPI(title="AQACS DEV API", version="0.0.1")
app.include_router(v1.router, prefix="/v1")

@app.get("/v1/health")
def health():
    return {
        "status": "ok",
        "env": "dev",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
