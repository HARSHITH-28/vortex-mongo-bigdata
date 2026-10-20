"""
VORTEX DATA LAKEHOUSE // EXPORT SERVICE
Data extraction & report generator (CSV, JSON, Markdown Analytics Brief)
"""
import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from .config import settings
from .database import db_manager
from .analytics import analytics_engine

logger = logging.getLogger("vortex.export")


class ExportService:
    """
    Exports MongoDB big data documents and analytical results in multiple formats.
    """

    @staticmethod
    def export_csv(limit: int = 5000) -> str:
        """
        Exports telemetry documents to CSV format.
        """
        if db_manager.db is None:
            return ""

        raw_coll = db_manager.db[settings.COLLECTION_RAW_TELEMETRY]
        cursor = raw_coll.find({}).sort("timestamp", -1).limit(limit)

        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow([
            "event_id", "timestamp", "source_node", "region", "category",
            "throughput_mbps", "latency_ms", "cpu_utilization", "memory_mb",
            "error_count", "temperature_c", "status", "tags"
        ])

        for doc in cursor:
            metrics = doc.get("metrics", {})
            ts = doc.get("timestamp")
            ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)
            tags_str = "|".join(doc.get("tags", []))
            
            writer.writerow([
                doc.get("event_id", ""),
                ts_str,
                doc.get("source_node", ""),
                doc.get("region", ""),
                doc.get("category", ""),
                metrics.get("throughput_mbps", 0),
                metrics.get("latency_ms", 0),
                metrics.get("cpu_utilization", 0),
                metrics.get("memory_mb", 0),
                metrics.get("error_count", 0),
                metrics.get("temperature_c", 0),
                doc.get("status", ""),
                tags_str
            ])

        return output.getvalue()

    @staticmethod
    def export_json(limit: int = 2000) -> List[Dict[str, Any]]:
        """
        Exports telemetry documents to sanitized JSON.
        """
        if db_manager.db is None:
            return []

        raw_coll = db_manager.db[settings.COLLECTION_RAW_TELEMETRY]
        cursor = raw_coll.find({}).sort("timestamp", -1).limit(limit)

        records = []
        for doc in cursor:
            doc_copy = dict(doc)
            if "_id" in doc_copy:
                doc_copy["_id"] = str(doc_copy["_id"])
            if isinstance(doc_copy.get("timestamp"), datetime):
                doc_copy["timestamp"] = doc_copy["timestamp"].isoformat()
            records.append(doc_copy)

        return records

    @staticmethod
    def export_markdown_report() -> str:
        """
        Generates an executive analytical Markdown summary of the lakehouse.
        """
        summary = analytics_engine.get_executive_summary()
        insights = analytics_engine.generate_automated_insights()
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        lines = [
            "# VORTEX // MongoDB Big Data Analytics & Telemetry Report",
            f"**Generated:** {now_str}  ",
            f"**Database Engine:** MongoDB ({settings.DATABASE_NAME})  ",
            f"**Storage Footprint:** {summary.get('estimated_size_mb', 0)} MB across {summary.get('total_documents', 0):,} events\n",
            "---",
            "## 1. Executive Performance Metrics",
            f"- **Total Documents Stored:** `{summary.get('total_documents', 0):,}`",
            f"- **Average Cluster Latency:** `{summary.get('avg_latency_ms', 0)} ms`",
            f"- **P95 Latency SLA:** `{summary.get('p95_latency_ms', 0)} ms`",
            f"- **P99 Latency SLA:** `{summary.get('p99_latency_ms', 0)} ms`",
            f"- **Average Bandwidth Throughput:** `{summary.get('avg_throughput_mbps', 0)} Mbps`",
            f"- **Average CPU Utilization:** `{summary.get('avg_cpu_percent', 0)}%`",
            f"- **Hardware Anomaly Incidents:** `{summary.get('critical_events', 0)} critical / {summary.get('warning_events', 0)} warnings`",
            f"- **Overall Health Score:** `{summary.get('health_score_pct', 0)}%`\n",
            "---",
            "## 2. Automated Actionable Insights",
        ]

        for ins in insights:
            lines.append(f"### [{ins.get('badge', 'NOTICE')}] {ins.get('title', '')}")
            lines.append(f"> {ins.get('summary', '')}")
            lines.append(f"**Action Item:** {ins.get('recommendation', '')}\n")

        lines.extend([
            "---",
            "## 3. MongoDB Aggregation Pipeline Architecture",
            "The lakehouse utilizes compound BSON indexing `(timestamp, region, category)` paired with dynamic multi-stage aggregations (`$match`, `$group`, `$facet`, `$bucketAuto`) to maintain sub-10ms response latencies at scale.",
            "\n*Report compiled autonomously by VORTEX Big Data Engine.*"
        ])

        return "\n".join(lines)


export_service = ExportService()
