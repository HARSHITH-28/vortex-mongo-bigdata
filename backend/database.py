"""
VORTEX DATA LAKEHOUSE // DATABASE LAYER
Resilient MongoDB Connection Manager supporting Live Cluster & Embedded High-Speed Engine
"""
import logging
import time
from typing import Dict, Any, Optional
import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import mongomock

from .config import settings

logger = logging.getLogger("vortex.database")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class DatabaseManager:
    """
    Manages MongoDB client lifecycle, dynamic failover, indexing,
    and performance statistics.
    """
    def __init__(self):
        self.client: Optional[MongoClient] = None
        self.db = None
        self.is_connected: bool = False
        self.is_mock: bool = False
        self.connection_uri: str = settings.MONGODB_URI
        self.ping_latency_ms: float = 0.0
        self.connect()

    def connect(self, uri: Optional[str] = None) -> Dict[str, Any]:
        """
        Attempts to connect to live MongoDB. Falls back to in-memory mongomock
        if unavailable to guarantee zero setup disruption.
        """
        target_uri = uri or self.connection_uri
        self.connection_uri = target_uri
        
        logger.info(f"Attempting MongoDB connection to: {self._mask_uri(target_uri)}")
        start_time = time.time()
        
        try:
            # Try connecting with a 2-second timeout
            test_client = MongoClient(
                target_uri,
                serverSelectionTimeoutMS=2000,
                connectTimeoutMS=2000,
                maxPoolSize=100,
                retryWrites=True,
                w=1
            )
            # Ping database to verify live responsiveness
            test_client.admin.command('ping')
            self.ping_latency_ms = round((time.time() - start_time) * 1000, 2)
            
            self.client = test_client
            self.db = self.client[settings.DATABASE_NAME]
            self.is_connected = True
            self.is_mock = False
            logger.info(f"Connected to LIVE MongoDB cluster ({self.ping_latency_ms}ms ping)")
            self._setup_indexes()
            return {
                "status": "connected",
                "mode": "live",
                "ping_ms": self.ping_latency_ms,
                "database": settings.DATABASE_NAME,
                "message": f"Successfully connected to live MongoDB ({self.ping_latency_ms}ms)"
            }
        except (ConnectionFailure, ServerSelectionTimeoutError, Exception) as e:
            logger.warning(f"Could not connect to live MongoDB ({str(e)}). Engaging Embedded High-Speed Engine (mongomock).")
            # Preserve existing mock data if already initialized, otherwise create new
            if not self.is_mock or self.client is None:
                self.client = mongomock.MongoClient()
                self.db = self.client[settings.DATABASE_NAME]
            self.is_connected = True
            self.is_mock = True
            self.ping_latency_ms = 0.1
            self._setup_indexes()
            return {
                "status": "connected",
                "mode": "embedded-mock",
                "ping_ms": 0.1,
                "database": settings.DATABASE_NAME,
                "message": "Connected via Embedded Zero-Config MongoDB Engine. Connect live MongoDB/Atlas anytime in UI."
            }

    def _setup_indexes(self):
        """
        Creates optimized compound indexes for Big Data analytical queries:
        - (timestamp DESC, region ASC)
        - (category ASC, timestamp DESC)
        - (metrics.latency_ms DESC)
        - TTL index simulation
        """
        if self.db is None:
            return
            
        try:
            raw_coll = self.db[settings.COLLECTION_RAW_TELEMETRY]
            # Compound index for time-series range and regional filtering
            raw_coll.create_index([("timestamp", DESCENDING), ("region", ASCENDING)])
            # Compound index for metric and category segmentation
            raw_coll.create_index([("category", ASCENDING), ("timestamp", DESCENDING)])
            # Index for fast outlier/latency scans
            raw_coll.create_index([("metrics.latency_ms", DESCENDING)])
            raw_coll.create_index([("status", ASCENDING)])
            
            # Hourly rollups indexing
            rollup_coll = self.db[settings.COLLECTION_ROLLUPS_HOURLY]
            rollup_coll.create_index([("bucket_start", DESCENDING), ("region", ASCENDING)], unique=True)
            
            # Anomalies indexing
            anomaly_coll = self.db[settings.COLLECTION_ANOMALIES]
            anomaly_coll.create_index([("detected_at", DESCENDING), ("severity", ASCENDING)])
            
            logger.info("MongoDB analytical compound indexes successfully verified/created.")
        except Exception as ex:
            logger.warning(f"Index creation notice: {ex}")

    def get_collection_stats(self) -> Dict[str, Any]:
        """
        Retrieves real-time document counts and storage statistics across collections.
        """
        if self.db is None:
            return {"error": "Database not initialized"}

        try:
            raw_coll = self.db[settings.COLLECTION_RAW_TELEMETRY]
            rollup_coll = self.db[settings.COLLECTION_ROLLUPS_HOURLY]
            anomaly_coll = self.db[settings.COLLECTION_ANOMALIES]
            
            raw_count = raw_coll.count_documents({})
            rollup_count = rollup_coll.count_documents({})
            anomaly_count = anomaly_coll.count_documents({})
            
            # Approximate storage footprint
            doc_size_est_bytes = 480  # average BSON size per telemetry document
            total_bytes = raw_count * doc_size_est_bytes
            mb_size = round(total_bytes / (1024 * 1024), 2)
            
            return {
                "raw_documents_count": raw_count,
                "rollups_count": rollup_count,
                "anomalies_count": anomaly_count,
                "estimated_size_mb": mb_size,
                "mode": "live" if not self.is_mock else "embedded-mock",
                "ping_latency_ms": self.ping_latency_ms,
                "is_connected": self.is_connected,
                "database_name": settings.DATABASE_NAME
            }
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            return {
                "raw_documents_count": 0,
                "rollups_count": 0,
                "anomalies_count": 0,
                "estimated_size_mb": 0.0,
                "mode": "unknown",
                "error": str(e)
            }

    def clear_collections(self) -> Dict[str, Any]:
        """
        Truncates telemetry, rollup, and anomaly collections for fresh testing.
        """
        if self.db is None:
            return {"deleted": 0}
            
        r1 = self.db[settings.COLLECTION_RAW_TELEMETRY].delete_many({})
        r2 = self.db[settings.COLLECTION_ROLLUPS_HOURLY].delete_many({})
        r3 = self.db[settings.COLLECTION_ANOMALIES].delete_many({})
        
        return {
            "deleted_raw": r1.deleted_count,
            "deleted_rollups": r2.deleted_count,
            "deleted_anomalies": r3.deleted_count
        }

    def _mask_uri(self, uri: str) -> str:
        """Mask credentials in connection string for security"""
        if "@" in uri:
            parts = uri.split("@")
            prefix = parts[0].split("//")[0] + "//***:***@"
            return prefix + parts[1]
        return uri

# Global singleton
db_manager = DatabaseManager()
