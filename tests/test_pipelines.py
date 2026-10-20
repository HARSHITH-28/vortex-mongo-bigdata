"""
VORTEX // UNIT & INTEGRATION TESTS
Automated validation of database connections, bulk ingestion, and pipelines
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.database import db_manager
from backend.ingestion import ingestion_engine
from backend.pipelines import pipelines
from backend.analytics import analytics_engine

def test_database_connection():
    stats = db_manager.get_collection_stats()
    assert stats["is_connected"] is True
    assert stats["mode"] in ["live", "embedded-mock"]

def test_bulk_ingestion():
    res = ingestion_engine.ingest_bulk_batch(count=200, anomaly_prob=0.1)
    assert res["status"] == "success"
    assert res["inserted_count"] == 200
    assert "throughput_ops_sec" in res

def test_time_series_aggregation():
    data = pipelines.run_time_series_aggregation(minutes_back=120)
    assert isinstance(data, list)

def test_custom_pipeline_execution():
    pipeline = [
        {"$group": {"_id": "$region", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    res = pipelines.execute_custom_pipeline(pipeline)
    assert res["status"] == "success"
    assert res["documents_returned"] > 0

def test_executive_summary():
    summary = analytics_engine.get_executive_summary()
    assert summary["total_documents"] >= 200
    assert 0 <= summary["health_score_pct"] <= 100

def test_automated_insights():
    insights = analytics_engine.generate_automated_insights()
    assert isinstance(insights, list)
    assert len(insights) > 0
