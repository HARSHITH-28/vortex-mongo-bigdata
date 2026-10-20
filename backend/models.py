"""
VORTEX DATA LAKEHOUSE // DATA MODELS
Pydantic schemas for Big Data ingestion, MongoDB document mapping & analytics
"""
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class TelemetryMetrics(BaseModel):
    throughput_mbps: float = Field(..., description="Network/data throughput in Megabits/sec")
    latency_ms: float = Field(..., description="Round-trip latency in milliseconds")
    cpu_utilization: float = Field(..., description="Processor load percentage 0-100")
    memory_mb: float = Field(..., description="Memory utilization in MB")
    error_count: int = Field(0, description="Error or packet loss count in interval")
    temperature_c: float = Field(..., description="Device or chassis temperature in Celsius")


class TelemetryDocument(BaseModel):
    event_id: str = Field(..., description="Unique UUID event identifier")
    timestamp: datetime = Field(..., description="Event generation timestamp")
    source_node: str = Field(..., description="Identifier of source cluster node or IoT device")
    region: str = Field(..., description="Geographic deployment zone")
    category: str = Field(..., description="Workload or telemetry category")
    metrics: TelemetryMetrics
    status: str = Field("OPTIMAL", description="Health status: OPTIMAL, WARNING, CRITICAL")
    tags: List[str] = Field(default_factory=list)
    payload_size_bytes: int = Field(512, description="Payload size in bytes")


class IngestionBatchRequest(BaseModel):
    count: int = Field(5000, ge=10, le=100000, description="Number of events to generate & store")
    category: Optional[str] = Field(None, description="Optional category filter, or random if None")
    anomaly_probability: float = Field(0.04, ge=0.0, le=0.5, description="Chance of synthetic anomaly injection")


class StreamControlRequest(BaseModel):
    events_per_second: int = Field(250, ge=10, le=5000)
    category: Optional[str] = None
    anomaly_probability: float = Field(0.03, ge=0.0, le=0.3)


class PipelineExecutionRequest(BaseModel):
    pipeline_json: List[Dict[str, Any]] = Field(..., description="Raw MongoDB aggregation pipeline stages")
    collection_name: Optional[str] = Field(None, description="Target collection name (defaults to telemetry_stream)")


class MongoConnectRequest(BaseModel):
    uri: str = Field(..., description="MongoDB Connection URI (e.g. mongodb://localhost:27017 or Atlas SRV)")


class HourlyRollupDocument(BaseModel):
    bucket_start: datetime
    bucket_end: datetime
    region: str
    category: str
    total_events: int
    avg_latency_ms: float
    max_latency_ms: float
    p95_latency_ms: float
    avg_throughput_mbps: float
    total_errors: int
    anomalous_events: int


class AnomalyRecord(BaseModel):
    event_id: str
    timestamp: datetime
    source_node: str
    region: str
    category: str
    metric_name: str
    observed_value: float
    z_score: float
    severity: str  # ELEVATED, HIGH, CRITICAL
    description: str
