"""
VORTEX DATA LAKEHOUSE // AGGREGATION PIPELINE REPOSITORY
Advanced multi-stage MongoDB pipelines for big data analytics, rollups & statistical mining
"""
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from .config import settings
from .database import db_manager

logger = logging.getLogger("vortex.pipelines")


class AggregationPipelines:
    """
    Houses optimized MongoDB Aggregation pipelines for telemetry big data processing.
    """

    @staticmethod
    def get_presets() -> List[Dict[str, Any]]:
        """
        Returns catalog of predefined advanced pipelines for the UI Pipeline Studio.
        """
        return [
            {
                "id": "time_series_5min",
                "name": "5-Minute Multi-Metric Rollup",
                "description": "Buckets high-velocity telemetry into 5-minute intervals, computing moving averages, peak latencies, and error totals.",
                "stages": [
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d %H:%M",
                                    "date": "$timestamp"
                                }
                            },
                            "event_count": {"$sum": 1},
                            "avg_latency": {"$avg": "$metrics.latency_ms"},
                            "max_latency": {"$max": "$metrics.latency_ms"},
                            "avg_throughput": {"$avg": "$metrics.throughput_mbps"},
                            "avg_cpu": {"$avg": "$metrics.cpu_utilization"},
                            "total_errors": {"$sum": "$metrics.error_count"}
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {"$limit": 50}
                ]
            },
            {
                "id": "regional_faceted_matrix",
                "name": "Multi-Faceted Regional & Category Matrix ($facet)",
                "description": "Executes simultaneous multi-dimensional analytical breakdowns across regions, categories, and severity tiers in one query.",
                "stages": [
                    {
                        "$facet": {
                            "by_region": [
                                {
                                    "$group": {
                                        "_id": "$region",
                                        "total_events": {"$sum": 1},
                                        "avg_latency": {"$avg": "$metrics.latency_ms"},
                                        "error_count": {"$sum": "$metrics.error_count"}
                                    }
                                },
                                {"$sort": {"total_events": -1}}
                            ],
                            "by_category": [
                                {
                                    "$group": {
                                        "_id": "$category",
                                        "total_events": {"$sum": 1},
                                        "avg_throughput": {"$avg": "$metrics.throughput_mbps"},
                                        "avg_cpu": {"$avg": "$metrics.cpu_utilization"}
                                    }
                                },
                                {"$sort": {"total_events": -1}}
                            ],
                            "by_status": [
                                {
                                    "$group": {
                                        "_id": "$status",
                                        "count": {"$sum": 1}
                                    }
                                }
                            ]
                        }
                    }
                ]
            },
            {
                "id": "critical_outlier_nodes",
                "name": "Top Degraded Source Nodes & Outliers",
                "description": "Identifies the highest-risk cluster nodes causing anomalous latencies or sustained hardware distress.",
                "stages": [
                    {
                        "$match": {
                            "$or": [
                                {"status": "CRITICAL"},
                                {"metrics.latency_ms": {"$gt": 250}},
                                {"metrics.cpu_utilization": {"$gt": 90}}
                            ]
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "source_node": "$source_node",
                                "region": "$region"
                            },
                            "critical_events": {"$sum": 1},
                            "max_latency_observed": {"$max": "$metrics.latency_ms"},
                            "max_cpu_observed": {"$max": "$metrics.cpu_utilization"},
                            "total_errors": {"$sum": "$metrics.error_count"}
                        }
                    },
                    {"$sort": {"critical_events": -1}},
                    {"$limit": 15}
                ]
            },
            {
                "id": "latency_distribution_buckets",
                "name": "Latency Distribution Auto-Bucketing ($bucketAuto)",
                "description": "Automatically distributes telemetry latencies into 6 equal-frequency histogram buckets to analyze SLA adherence.",
                "stages": [
                    {
                        "$bucketAuto": {
                            "groupBy": "$metrics.latency_ms",
                            "buckets": 6,
                            "output": {
                                "count": {"$sum": 1},
                                "avg_cpu": {"$avg": "$metrics.cpu_utilization"},
                                "avg_throughput": {"$avg": "$metrics.throughput_mbps"}
                            }
                        }
                    }
                ]
            }
        ]

    @staticmethod
    def run_time_series_aggregation(minutes_back: int = 60, bucket_size_min: int = 2) -> List[Dict[str, Any]]:
        """
        Aggregates recent telemetry into time slices for charting.
        """
        if db_manager.db is None:
            return []

        coll = db_manager.db[settings.COLLECTION_RAW_TELEMETRY]
        since_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_back)

        pipeline = [
            {"$match": {"timestamp": {"$gte": since_time}}},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%H:%M",
                            "date": "$timestamp"
                        }
                    },
                    "timestamp_bucket": {"$first": "$timestamp"},
                    "count": {"$sum": 1},
                    "avg_latency": {"$avg": "$metrics.latency_ms"},
                    "max_latency": {"$max": "$metrics.latency_ms"},
                    "avg_throughput": {"$avg": "$metrics.throughput_mbps"},
                    "avg_cpu": {"$avg": "$metrics.cpu_utilization"},
                    "total_errors": {"$sum": "$metrics.error_count"},
                    "critical_count": {
                        "$sum": {"$cond": [{"$eq": ["$status", "CRITICAL"]}, 1, 0]}
                    }
                }
            },
            {"$sort": {"_id": 1}}
        ]

        try:
            results = list(coll.aggregate(pipeline))
            # Format results
            formatted = []
            for item in results:
                formatted.append({
                    "time": item["_id"],
                    "count": item.get("count", 0),
                    "avg_latency": round(item.get("avg_latency", 0) or 0, 2),
                    "max_latency": round(item.get("max_latency", 0) or 0, 2),
                    "avg_throughput": round(item.get("avg_throughput", 0) or 0, 2),
                    "avg_cpu": round(item.get("avg_cpu", 0) or 0, 2),
                    "total_errors": item.get("total_errors", 0),
                    "critical_count": item.get("critical_count", 0)
                })
            return formatted
        except Exception as e:
            logger.error(f"Time-series aggregation failed: {e}")
            return []

    @staticmethod
    def run_regional_aggregation() -> List[Dict[str, Any]]:
        """
        Aggregates metrics grouped by geographic region.
        """
        if db_manager.db is None:
            return []

        coll = db_manager.db[settings.COLLECTION_RAW_TELEMETRY]
        pipeline = [
            {
                "$group": {
                    "_id": "$region",
                    "total_events": {"$sum": 1},
                    "avg_latency": {"$avg": "$metrics.latency_ms"},
                    "avg_throughput": {"$avg": "$metrics.throughput_mbps"},
                    "total_errors": {"$sum": "$metrics.error_count"},
                    "nodes": {"$addToSet": "$source_node"},
                    "critical_events": {
                        "$sum": {"$cond": [{"$eq": ["$status", "CRITICAL"]}, 1, 0]}
                    }
                }
            },
            {"$sort": {"total_events": -1}}
        ]

        try:
            results = list(coll.aggregate(pipeline))
            formatted = []
            for item in results:
                node_list = item.get("nodes", [])
                formatted.append({
                    "region": item["_id"],
                    "total_events": item.get("total_events", 0),
                    "avg_latency": round(item.get("avg_latency", 0) or 0, 2),
                    "avg_throughput": round(item.get("avg_throughput", 0) or 0, 2),
                    "total_errors": item.get("total_errors", 0),
                    "active_nodes_count": len(node_list) if isinstance(node_list, list) else 0,
                    "critical_events": item.get("critical_events", 0)
                })
            return formatted
        except Exception as e:
            logger.error(f"Regional aggregation failed: {e}")
            return []

    @staticmethod
    def execute_custom_pipeline(
        pipeline: List[Dict[str, Any]], 
        collection_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Executes a custom user-defined aggregation pipeline against MongoDB.
        Measures exact query execution time and documents returned.
        """
        if db_manager.db is None:
            return {"error": "Database is not connected"}

        target_coll_name = collection_name or settings.COLLECTION_RAW_TELEMETRY
        coll = db_manager.db[target_coll_name]

        start_time = time.time()
        try:
            cursor = coll.aggregate(pipeline, allowDiskUse=True)
            results = list(cursor)
            exec_time_ms = round((time.time() - start_time) * 1000, 2)

            # Sanitize ObjectId and Datetime for JSON response
            sanitized = []
            for doc in results[:200]:  # Cap output at 200 items for frontend responsiveness
                sanitized.append(AggregationPipelines._sanitize_bson(doc))

            return {
                "status": "success",
                "execution_time_ms": exec_time_ms,
                "documents_returned": len(results),
                "preview_count": len(sanitized),
                "collection": target_coll_name,
                "data": sanitized
            }
        except Exception as e:
            logger.error(f"Custom pipeline execution error: {e}")
            return {
                "status": "error",
                "execution_time_ms": round((time.time() - start_time) * 1000, 2),
                "error_message": str(e),
                "data": []
            }

    @staticmethod
    def _sanitize_bson(item: Any) -> Any:
        """Recursively converts BSON datatypes (ObjectId, datetime) to JSON serializable forms"""
        if isinstance(item, dict):
            return {k: AggregationPipelines._sanitize_bson(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [AggregationPipelines._sanitize_bson(v) for v in item]
        elif isinstance(item, datetime):
            return item.isoformat()
        elif hasattr(item, "__str__") and type(item).__name__ == "ObjectId":
            return str(item)
        return item


pipelines = AggregationPipelines()
