"""
VORTEX // CLI BIG DATA SEED TOOL
Generates and loads massive datasets into MongoDB with throughput measurement
"""
import sys
import os
import time
import argparse

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.database import db_manager
from backend.ingestion import ingestion_engine

def main():
    parser = argparse.ArgumentParser(description="Seed Big Data into MongoDB")
    parser.add_argument("--count", type=int, default=10000, help="Number of telemetry events (e.g. 10000)")
    parser.add_argument("--anomaly-prob", type=float, default=0.04, help="Anomaly injection probability (0.0 to 0.5)")
    args = parser.parse_args()

    print("=" * 60)
    print(f"[*] VORTEX BIG DATA SEED ENGINE")
    print(f"Target count: {args.count:,} events")
    print(f"Anomaly rate: {args.anomaly_prob*100:.1f}%")
    print(f"MongoDB Mode: {db_manager.get_collection_stats()['mode']}")
    print("=" * 60)

    start = time.time()
    res = ingestion_engine.ingest_bulk_batch(
        count=args.count,
        anomaly_prob=args.anomaly_prob
    )

    elapsed = time.time() - start
    print(f"[+] Seeding Complete!")
    print(f"Inserted: {res.get('inserted_count', 0):,} documents")
    print(f"Anomalies Flagged: {res.get('anomalies_flagged', 0):,}")
    print(f"Total Time: {elapsed:.2f} seconds")
    print(f"Throughput: {res.get('throughput_ops_sec', 0):,.1f} docs/sec")
    print("=" * 60)

if __name__ == "__main__":
    main()
