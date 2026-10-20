"""
VORTEX DATA LAKEHOUSE // CONFIGURATION
MongoDB Big Data Processing & Real-Time Analytics Matrix
"""
import os
from typing import Dict, Any

class Settings:
    PROJECT_NAME: str = "VORTEX // MongoDB Big Data Engine"
    VERSION: str = "2.4.0-ULTRA"
    DESCRIPTION: str = "High-throughput MongoDB telemetry lakehouse, batch/stream processing & statistical analytics"
    
    # MongoDB Connection Configuration
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "vortex_bigdata")
    
    # Collections
    COLLECTION_RAW_TELEMETRY: str = "telemetry_stream"
    COLLECTION_ROLLUPS_HOURLY: str = "telemetry_hourly_rollups"
    COLLECTION_ANOMALIES: str = "telemetry_anomalies"
    COLLECTION_PIPELINE_JOBS: str = "pipeline_execution_jobs"
    
    # Server Configuration
    HOST: str = os.getenv("HOST", "127.0.0.1")
    PORT: int = int(os.getenv("PORT", "8080"))
    
    # Ingestion & Big Data Thresholds
    DEFAULT_BATCH_SIZE: int = 1000
    MAX_BATCH_SIZE: int = 25000
    DEFAULT_STREAM_RATE_PER_SEC: int = 250
    ANOMALY_Z_SCORE_THRESHOLD: float = 2.58  # 99% confidence interval
    
    # Regions & Categories for realistic simulation
    REGIONS: list[str] = [
        "us-east-virginia", "us-west-oregon", "eu-west-frankfurt", 
        "ap-southeast-singapore", "ap-northeast-tokyo", "sa-east-saopaulo"
    ]
    CATEGORIES: list[str] = [
        "iot_industrial_sensor", "cloud_hypervisor_telemetry", 
        "financial_orderbook_tick", "edge_mesh_gateway", "cyber_security_audit"
    ]

settings = Settings()
