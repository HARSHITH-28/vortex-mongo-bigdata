"""
VORTEX DATA LAKEHOUSE // SYNTHETIC BIG DATA GENERATOR
High-velocity telemetry generator with physical modeling & statistical anomalies
"""
import random
import uuid
import math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from .config import settings


class TelemetryDataGenerator:
    """
    Generates realistic, multi-dimensional Big Data telemetry events.
    Models normal operational variance, correlation between metrics,
    and synthetic anomaly injections.
    """
    def __init__(self):
        self.node_registry = [
            f"edge-vortex-{region[:3]}-{i:03d}" 
            for region in settings.REGIONS 
            for i in range(1, 15)
        ]
        
    def generate_single_event(
        self, 
        timestamp: Optional[datetime] = None, 
        category: Optional[str] = None,
        inject_anomaly: bool = False
    ) -> Dict[str, Any]:
        """
        Creates a single realistic telemetry document.
        """
        ts = timestamp or datetime.now(timezone.utc)
        selected_category = category or random.choice(settings.CATEGORIES)
        selected_region = random.choice(settings.REGIONS)
        
        # Pick node matching the region
        matching_nodes = [n for n in self.node_registry if selected_region[:3] in n]
        source_node = random.choice(matching_nodes) if matching_nodes else f"node-{random.randint(100, 999)}"

        # Base physical parameters
        base_latency = random.uniform(12.0, 48.0)
        base_throughput = random.uniform(150.0, 950.0)
        base_cpu = random.uniform(18.0, 62.0)
        base_memory = random.uniform(1024.0, 8192.0)
        base_temp = random.uniform(38.0, 64.0)
        error_count = 0
        status = "OPTIMAL"

        # Apply realistic diurnal / sine modulation based on hour
        hour_factor = math.sin((ts.hour / 24.0) * 2 * math.pi)
        base_throughput += hour_factor * 80.0
        base_cpu += hour_factor * 10.0

        tags = [selected_category.split("_")[0], selected_region.split("-")[0]]

        # Inject anomalies if requested
        if inject_anomaly:
            anomaly_type = random.choice([
                "LATENCY_SPIKE", "CPU_OVERLOAD", "PACKET_BURST_FAILURE", 
                "THERMAL_EXCURSION", "MEMORY_LEAK_SPIKE"
            ])
            tags.append("ANOMALY")
            tags.append(anomaly_type)
            
            if anomaly_type == "LATENCY_SPIKE":
                base_latency = random.uniform(350.0, 1400.0)
                status = "CRITICAL" if base_latency > 700 else "WARNING"
            elif anomaly_type == "CPU_OVERLOAD":
                base_cpu = random.uniform(92.0, 99.8)
                base_temp = random.uniform(78.0, 94.0)
                status = "CRITICAL"
            elif anomaly_type == "PACKET_BURST_FAILURE":
                error_count = random.randint(15, 240)
                base_throughput = random.uniform(10.0, 50.0)
                status = "CRITICAL"
            elif anomaly_type == "THERMAL_EXCURSION":
                base_temp = random.uniform(85.0, 102.0)
                status = "WARNING"
            elif anomaly_type == "MEMORY_LEAK_SPIKE":
                base_memory = random.uniform(14000.0, 16384.0)
                base_cpu = random.uniform(75.0, 91.0)
                status = "WARNING"

        return {
            "event_id": f"evt-{uuid.uuid4().hex[:12]}",
            "timestamp": ts,
            "source_node": source_node,
            "region": selected_region,
            "category": selected_category,
            "metrics": {
                "throughput_mbps": round(max(1.0, base_throughput), 2),
                "latency_ms": round(max(1.0, base_latency), 2),
                "cpu_utilization": round(min(100.0, max(1.0, base_cpu)), 2),
                "memory_mb": round(max(256.0, base_memory), 2),
                "error_count": int(error_count),
                "temperature_c": round(max(20.0, base_temp), 2)
            },
            "status": status,
            "tags": tags,
            "payload_size_bytes": random.randint(320, 1480)
        }

    def generate_batch(
        self, 
        count: int = 5000, 
        category: Optional[str] = None, 
        anomaly_probability: float = 0.04,
        time_spread_hours: float = 4.0
    ) -> List[Dict[str, Any]]:
        """
        Generates a bulk list of documents spread across a time window.
        """
        now = datetime.now(timezone.utc)
        docs = []
        for _ in range(count):
            # Spread randomly over the time window
            seconds_ago = random.uniform(0, time_spread_hours * 3600)
            doc_time = now - timedelta(seconds=seconds_ago)
            inject = random.random() < anomaly_probability
            doc = self.generate_single_event(
                timestamp=doc_time, 
                category=category, 
                inject_anomaly=inject
            )
            docs.append(doc)
            
        return docs


# Singleton generator instance
generator = TelemetryDataGenerator()
