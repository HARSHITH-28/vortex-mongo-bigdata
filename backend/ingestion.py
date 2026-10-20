"""
VORTEX DATA LAKEHOUSE // INGESTION ENGINE
High-throughput asynchronous & batch ingestion pipeline for MongoDB
"""
import asyncio
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from .config import settings
from .database import db_manager
from .data_generator import generator

logger = logging.getLogger("vortex.ingestion")


class IngestionEngine:
    """
    Manages high-throughput Big Data ingestion into MongoDB.
    Supports micro-batch streaming, concurrent bulk inserts,
    and real-time ops/sec monitoring.
    """
    def __init__(self):
        self.is_streaming: bool = False
        self.stream_thread: Optional[threading.Thread] = None
        self.stream_rate: int = settings.DEFAULT_STREAM_RATE_PER_SEC
        self.anomaly_prob: float = 0.04
        self.stream_category: Optional[str] = None
        
        # Real-time metrics
        self.total_ingested_session: int = 0
        self.current_ops_sec: float = 0.0
        self.last_batch_latency_ms: float = 0.0
        self.recent_events_buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def start_streaming(self, rate_per_sec: int = 250, category: Optional[str] = None, anomaly_prob: float = 0.04):
        """
        Starts the background streaming ingestion worker.
        """
        if self.is_streaming:
            self.stop_streaming()

        self.stream_rate = min(max(10, rate_per_sec), 5000)
        self.stream_category = category
        self.anomaly_prob = anomaly_prob
        self.is_streaming = True

        self.stream_thread = threading.Thread(target=self._streaming_worker, daemon=True)
        self.stream_thread.start()
        logger.info(f"Ingestion Stream STARTED @ {self.stream_rate} events/sec (Anomaly Rate: {self.anomaly_prob*100}%)")

    def stop_streaming(self):
        """
        Stops the streaming worker.
        """
        self.is_streaming = False
        if self.stream_thread and self.stream_thread.is_alive():
            self.stream_thread.join(timeout=1.5)
        self.current_ops_sec = 0.0
        logger.info("Ingestion Stream STOPPED")

    def _streaming_worker(self):
        """
        Micro-batch streaming loop. Runs in background thread.
        Emits micro-batches every 200ms to balance throughput and MongoDB write efficiency.
        """
        interval_sec = 0.2
        batch_size = max(5, int(self.stream_rate * interval_sec))

        while self.is_streaming:
            start_t = time.time()
            try:
                # Generate micro-batch
                batch = []
                anomalies = []
                now = datetime.now(timezone.utc)
                
                for _ in range(batch_size):
                    inject = (time.time() % 1.0 < self.anomaly_prob)
                    doc = generator.generate_single_event(
                        timestamp=now,
                        category=self.stream_category,
                        inject_anomaly=inject
                    )
                    batch.append(doc)
                    if doc.get("status") in ["WARNING", "CRITICAL"]:
                        anomalies.append({
                            "event_id": doc["event_id"],
                            "timestamp": doc["timestamp"],
                            "source_node": doc["source_node"],
                            "region": doc["region"],
                            "category": doc["category"],
                            "severity": doc["status"],
                            "metrics": doc["metrics"],
                            "tags": doc["tags"]
                        })

                # Bulk insert into MongoDB
                if db_manager.db is not None and batch:
                    raw_coll = db_manager.db[settings.COLLECTION_RAW_TELEMETRY]
                    insert_start = time.time()
                    raw_coll.insert_many(batch, ordered=False)
                    self.last_batch_latency_ms = round((time.time() - insert_start) * 1000, 2)

                    # Also register anomalies if any
                    if anomalies:
                        anom_coll = db_manager.db[settings.COLLECTION_ANOMALIES]
                        anom_coll.insert_many(anomalies, ordered=False)

                    with self._lock:
                        self.total_ingested_session += len(batch)
                        # Keep last 20 events for live HUD visualization
                        self.recent_events_buffer = (batch[-20:] + self.recent_events_buffer)[:20]

                    # Cap in-memory mock collection to protect free-tier RAM if in mock mode
                    if db_manager.is_mock and self.total_ingested_session % 500 == 0:
                        if raw_coll.count_documents({}) > 6000:
                            oldest_ids = [d["_id"] for d in raw_coll.find({}, {"_id": 1}).limit(1500)]
                            raw_coll.delete_many({"_id": {"$in": oldest_ids}})

            except Exception as e:
                logger.error(f"Error in streaming worker: {e}")

            elapsed = time.time() - start_t
            if elapsed > 0:
                self.current_ops_sec = round(len(batch) / elapsed, 1)

            sleep_time = max(0.01, interval_sec - elapsed)
            time.sleep(sleep_time)

    def ingest_bulk_batch(
        self, 
        count: int = 5000, 
        category: Optional[str] = None, 
        anomaly_prob: float = 0.04
    ) -> Dict[str, Any]:
        """
        Executes a single high-speed bulk ingestion batch.
        Ideal for seeding millions/thousands of historical data points.
        """
        if db_manager.db is None:
            return {"error": "Database unavailable"}

        start_time = time.time()
        logger.info(f"Generating and bulk inserting {count} documents...")
        
        docs = generator.generate_batch(
            count=count, 
            category=category, 
            anomaly_probability=anomaly_prob
        )
        gen_time = time.time() - start_time
        
        insert_start = time.time()
        raw_coll = db_manager.db[settings.COLLECTION_RAW_TELEMETRY]
        
        # Micro-chunk for memory safety on huge batches
        chunk_size = 5000
        inserted_count = 0
        anomalies = []

        for i in range(0, len(docs), chunk_size):
            chunk = docs[i:i + chunk_size]
            res = raw_coll.insert_many(chunk, ordered=False)
            inserted_count += len(res.inserted_ids)
            
            # Extract anomalies
            for doc in chunk:
                if doc.get("status") in ["WARNING", "CRITICAL"]:
                    anomalies.append({
                        "event_id": doc["event_id"],
                        "timestamp": doc["timestamp"],
                        "source_node": doc["source_node"],
                        "region": doc["region"],
                        "category": doc["category"],
                        "severity": doc["status"],
                        "metrics": doc["metrics"],
                        "tags": doc["tags"]
                    })

        # Insert anomalies
        if anomalies:
            anom_coll = db_manager.db[settings.COLLECTION_ANOMALIES]
            anom_coll.insert_many(anomalies, ordered=False)

        total_time = round(time.time() - start_time, 3)
        insert_time = round(time.time() - insert_start, 3)
        effective_throughput = round(inserted_count / total_time, 1) if total_time > 0 else 0

        with self._lock:
            self.total_ingested_session += inserted_count
            self.recent_events_buffer = docs[-20:]

        logger.info(f"Bulk Ingest Completed: {inserted_count} docs in {total_time}s ({effective_throughput} docs/sec)")
        
        return {
            "status": "success",
            "inserted_count": inserted_count,
            "anomalies_flagged": len(anomalies),
            "generation_time_sec": round(gen_time, 3),
            "mongo_insert_time_sec": insert_time,
            "total_elapsed_sec": total_time,
            "throughput_ops_sec": effective_throughput
        }

    def get_ingestion_telemetry(self) -> Dict[str, Any]:
        """
        Returns real-time status of the ingestion pipeline.
        """
        with self._lock:
            # Format recent events for JSON
            formatted_recent = []
            for ev in self.recent_events_buffer[:10]:
                formatted_recent.append({
                    "event_id": ev["event_id"],
                    "timestamp": ev["timestamp"].isoformat() if isinstance(ev["timestamp"], datetime) else str(ev["timestamp"]),
                    "source_node": ev["source_node"],
                    "region": ev["region"],
                    "category": ev["category"],
                    "latency_ms": ev["metrics"]["latency_ms"],
                    "throughput_mbps": ev["metrics"]["throughput_mbps"],
                    "status": ev["status"]
                })

        return {
            "is_streaming": self.is_streaming,
            "stream_rate_target": self.stream_rate,
            "current_ops_sec": self.current_ops_sec if self.is_streaming else 0.0,
            "last_batch_latency_ms": self.last_batch_latency_ms,
            "session_total_ingested": self.total_ingested_session,
            "recent_events": formatted_recent
        }


ingestion_engine = IngestionEngine()
