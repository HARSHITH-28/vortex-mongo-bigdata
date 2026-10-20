"""
VORTEX DATA LAKEHOUSE // MAIN FASTAPI & WEBSOCKET SERVER
Enterprise API Gateway for MongoDB Big Data Storage, Processing & Analytics
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, PlainTextResponse

from .config import settings
from .database import db_manager
from .models import (
    IngestionBatchRequest, StreamControlRequest,
    PipelineExecutionRequest, MongoConnectRequest
)
from .ingestion import ingestion_engine
from .pipelines import pipelines
from .analytics import analytics_engine
from .export_service import export_service

logger = logging.getLogger("vortex.server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description=settings.DESCRIPTION
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connection manager for WebSockets
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

ws_manager = ConnectionManager()

# Background broadcast loop
@app.on_event("startup")
async def startup_event():
    logger.info("Initializing VORTEX Big Data Engine...")
    # Seed initial data if database is empty so user sees stunning graphs immediately!
    stats = db_manager.get_collection_stats()
    if stats.get("raw_documents_count", 0) == 0:
        logger.info("Database reservoir empty. Pre-seeding 2,500 initial telemetry points for immediate visualization...")
        ingestion_engine.ingest_bulk_batch(count=2500, anomaly_prob=0.04)

    # Launch background WebSocket broadcaster
    asyncio.create_task(websocket_telemetry_broadcaster())

async def websocket_telemetry_broadcaster():
    """
    Emits real-time telemetry pulse every 1 second to all connected dashboard clients.
    """
    while True:
        try:
            if ws_manager.active_connections:
                telemetry = ingestion_engine.get_ingestion_telemetry()
                stats = db_manager.get_collection_stats()
                
                payload = {
                    "type": "TELEMETRY_PULSE",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ingestion": telemetry,
                    "storage": stats
                }
                await ws_manager.broadcast(payload)
        except Exception as e:
            logger.error(f"WebSocket broadcast error: {e}")
        await asyncio.sleep(1.0)


# ==========================================
# REST API ENDPOINTS
# ==========================================

@app.get("/api/status")
async def get_system_status():
    """Get system health, MongoDB connection mode, and collection statistics."""
    stats = db_manager.get_collection_stats()
    telemetry = ingestion_engine.get_ingestion_telemetry()
    return {
        "project": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "database": stats,
        "ingestion": telemetry,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.post("/api/connect")
async def connect_mongo(req: MongoConnectRequest):
    """Dynamically connect to a specified MongoDB URI (Local or Atlas)."""
    res = db_manager.connect(req.uri)
    return res


@app.post("/api/ingestion/start")
async def start_stream(req: StreamControlRequest):
    """Start high-velocity background streaming ingestion."""
    ingestion_engine.start_streaming(
        rate_per_sec=req.events_per_second,
        category=req.category,
        anomaly_prob=req.anomaly_probability
    )
    return {"status": "started", "rate_per_sec": req.events_per_second}


@app.post("/api/ingestion/stop")
async def stop_stream():
    """Stop the background streaming worker."""
    ingestion_engine.stop_streaming()
    return {"status": "stopped"}


@app.post("/api/ingestion/batch")
async def ingest_batch(req: IngestionBatchRequest):
    """Run an immediate high-speed bulk ingestion batch."""
    res = ingestion_engine.ingest_bulk_batch(
        count=req.count,
        category=req.category,
        anomaly_prob=req.anomaly_probability
    )
    return res


@app.delete("/api/data/clear")
async def clear_database():
    """Truncate collections for testing reset."""
    ingestion_engine.stop_streaming()
    res = db_manager.clear_collections()
    return {"status": "cleared", "details": res}


@app.get("/api/analytics/overview")
async def get_analytics_overview():
    """Retrieve executive KPI summary and health score."""
    summary = analytics_engine.get_executive_summary()
    return summary


@app.get("/api/analytics/timeseries")
async def get_timeseries_analytics(minutes: int = Query(60, ge=5, le=1440)):
    """Retrieve time-series rollups for charts."""
    return pipelines.run_time_series_aggregation(minutes_back=minutes)


@app.get("/api/analytics/regions")
async def get_regional_analytics():
    """Retrieve regional distribution aggregations."""
    return pipelines.run_regional_aggregation()


@app.get("/api/analytics/anomalies")
async def get_anomalies(limit: int = Query(50, ge=1, le=200)):
    """Retrieve detected anomaly outliers."""
    return analytics_engine.get_recent_anomalies(limit=limit)


@app.get("/api/analytics/insights")
async def get_automated_insights():
    """Retrieve auto-generated executive intelligence findings."""
    return analytics_engine.generate_automated_insights()


@app.get("/api/pipelines/presets")
async def get_pipeline_presets():
    """Get catalog of ready-to-run advanced MongoDB aggregation pipelines."""
    return pipelines.get_presets()


@app.post("/api/pipelines/execute")
async def execute_custom_pipeline(req: PipelineExecutionRequest):
    """Execute arbitrary MongoDB aggregation pipeline with timing metrics."""
    res = pipelines.execute_custom_pipeline(req.pipeline_json, req.collection_name)
    return res


@app.get("/api/data/documents")
async def browse_documents(
    skip: int = Query(0, ge=0),
    limit: int = Query(25, ge=1, le=100),
    region: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None
):
    """Browse stored big data documents with pagination and filters."""
    if db_manager.db is None:
        return {"documents": [], "total": 0}

    query = {}
    if region:
        query["region"] = region
    if category:
        query["category"] = category
    if status:
        query["status"] = status

    raw_coll = db_manager.db[settings.COLLECTION_RAW_TELEMETRY]
    total = raw_coll.count_documents(query)
    cursor = raw_coll.find(query).sort("timestamp", -1).skip(skip).limit(limit)

    sanitized = []
    for doc in cursor:
        sanitized.append(pipelines._sanitize_bson(doc))

    return {
        "documents": sanitized,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@app.get("/api/export/csv")
async def export_data_csv(limit: int = Query(5000, ge=10, le=50000)):
    """Export big data events to CSV."""
    csv_content = export_service.export_csv(limit=limit)
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=vortex_bigdata_{int(datetime.now().timestamp())}.csv"}
    )


@app.get("/api/export/json")
async def export_data_json(limit: int = Query(2000, ge=10, le=10000)):
    """Export big data events to JSON."""
    json_data = export_service.export_json(limit=limit)
    return json_data


@app.get("/api/export/report")
async def export_analytical_report():
    """Export comprehensive Markdown analytical brief."""
    report_md = export_service.export_markdown_report()
    return PlainTextResponse(content=report_md, media_type="text/markdown")


# ==========================================
# WEBSOCKET STREAM
# ==========================================

@app.websocket("/ws/telemetry")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            # Keep connection alive & handle incoming client messages
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                # Handle client actions over websocket (e.g. ping)
                if msg.get("action") == "PING":
                    await websocket.send_json({"type": "PONG", "time": datetime.now(timezone.utc).isoformat()})
            except Exception:
                pass
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)


# ==========================================
# STATIC ASSETS & FRONTEND SERVING
# ==========================================

# Mount frontend directory
frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
async def serve_index():
    index_file = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "VORTEX Big Data Engine API is running. Frontend index.html not found."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.app:app", host=settings.HOST, port=settings.PORT, reload=True)
