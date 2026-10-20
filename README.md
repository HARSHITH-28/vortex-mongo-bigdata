# VORTEX // MongoDB Big Data Lakehouse & Real-Time Analytics

> **Autonomous High-Throughput Telemetry Ingestion, Aggregation Pipeline Engine & Statistical Anomaly Matrix powered by MongoDB.**

---

## 🌌 Overview

**VORTEX** is an enterprise-grade Big Data platform engineered to ingest, store, process, and analyze hyper-scale time-series telemetry using **MongoDB**. Designed around MongoDB's native Big Data storage patterns (Compound BSON Indexing, Time-Series Bucketing, and Multi-Stage Aggregation Pipelines), VORTEX provides real-time operational observability, automated statistical outlier detection, and an interactive Aggregation Studio.

The user interface follows a **Cyber-Industrial Spatial Telemetry design language** ("Obsidian Glassmorphism & Bioluminescent Luminescence") featuring real-time canvas particle physics, high-density KPI HUD dials, animated stream waterfalls, and procedural Web Audio feedback.

---

## ⚡ Key Features

### 1. High-Throughput MongoDB Storage Engine
- **Flexible Connection Architecture**: Connects seamlessly to any local `mongod` instance, Docker container, or **MongoDB Atlas** cluster. Includes an embedded zero-configuration engine fallback (`mongomock`) for immediate out-of-the-box operation with zero prior database setup.
- **Micro-Batch Bulk Ingestion**: Utilizes unordered BSON bulk inserts (`insert_many(ordered=False)`) delivering sustained throughput of **15,000+ documents/sec**.
- **Analytical Compound Indexing**:
  - `(timestamp: -1, region: 1)` — Optimal for temporal range scans and regional segmentation.
  - `(category: 1, timestamp: -1)` — Fast workload partitioning.
  - `(metrics.latency_ms: -1)` — Outlier and percentile scan optimization.
  - `(status: 1)` — Rapid health and anomaly index lookups.

### 2. Multi-Stage Aggregation Pipeline Engine
- **Real-Time 5-Minute Rollups**: Buckets high-velocity telemetry into time intervals calculating moving averages, peak latencies, and packet error sums.
- **Multi-Faceted Global Analysis (`$facet`)**: Executes simultaneous multi-branch aggregations across regions, categories, and severity tiers in a single database round-trip.
- **Latency SLA Quantiles (`$bucketAuto`)**: Distributes millions of events into equal-frequency histogram buckets to calculate P50, P90, P95, and P99 SLA adherence.
- **Interactive Pipeline Workbench**: Build and execute custom MongoDB aggregation pipelines in real-time JSON format, with millisecond execution profiling, document count verification, and dynamic auto-charting.

### 3. Statistical Anomaly & Outlier Matrix
- **Z-Score Detection**: Flags hardware distress and anomalous latency deviations where $Z > 2.58$ (99% confidence interval).
- **Automated Heuristic Intelligence**: Heuristic rules analyze database rollups and generate human-readable actionable findings (e.g. regional bottleneck warnings, single-node hardware failures, SLA status).

### 4. Enterprise Data Lake Explorer & Export
- **Document Browser**: Paginated inspection of stored BSON documents with real-time multi-attribute filtering.
- **Export Formats**: One-click extraction to **CSV**, sanitized **JSON**, and comprehensive **Executive Analytical Markdown Briefs**.

---

## 🏛️ System Architecture

```
                                [ EDGE TELEMETRY PRODUCERS ]
                  (IoT Sensors, Financial Ticks, Cloud Hypervisors, Mesh Gateways)
                                             │
                                             ▼
                             [ VORTEX INGESTION ENGINE ]
                     (Micro-Batching 200ms Buffer, Unordered Bulk API)
                                             │
                        ┌────────────────────┴────────────────────┐
                        ▼                                         ▼
            [ MONGODB LAKEHOUSE ]                      [ REAL-TIME WEBSOCKET ]
          telemetry_stream collection                    (/ws/telemetry)
       • Compound Indexes: (ts, reg, cat)                         │
       • Time-Series Bucketing                                    ▼
       • TTL Lifecycle Policy                           [ REAL-TIME HUD CLIENT ]
                        │                                (Live Ops/Sec, Waterfall)
                        ▼
         [ AGGREGATION PIPELINE ENGINE ]
       ($match, $group, $facet, $bucketAuto)
                        │
                        ▼
         [ STATISTICAL ANALYTICS ENGINE ]
          (Z-Scores, P95/P99, Heuristics)
                        │
                        ▼
       [ AESTHETIC SPATIAL DASHBOARD & STUDIO ]
```

---

## 🚀 Quick Start Guide

### Prerequisites
- **Python 3.10+** (Python 3.12 verified)
- *(Optional)* **MongoDB 6.0+** or **MongoDB Atlas URI** (Embedded engine runs automatically if MongoDB is not present)

### 1. Installation
Clone or navigate to the project directory:
```bash
cd vortex-mongo-bigdata
pip install -r requirements.txt
```

### 2. Start Application Server
```bash
# Option A: Windows Batch file
run.bat

# Option B: PowerShell
.\start.ps1

# Option C: Direct Python
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8080 --reload
```

Open your browser at **[http://127.0.0.1:8080](http://127.0.0.1:8080)**.

---

## 🛠️ CLI Tools & Automated Benchmarks

### 1. Bulk Big Data Seeder
Populate thousands of documents in seconds with configurable anomaly probability:
```bash
python scripts/seed_bigdata.py --count 25000 --anomaly-prob 0.05
```

### 2. Aggregation Benchmark Suite
Profile query execution latencies across count, rollup, `$facet`, and heuristic engines:
```bash
python scripts/run_benchmark.py
```

### 3. Automated Test Suite
Run unit and integration tests verifying database integrity and pipeline correctness:
```bash
python -m pytest tests/test_pipelines.py -v
```

---

## 📡 REST API & WebSocket Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/status` | Database connection mode, ping latency, doc counts, ingestion state |
| `POST` | `/api/connect` | Connect to live MongoDB cluster or MongoDB Atlas URI |
| `POST` | `/api/ingestion/start` | Start background streaming worker with rate & anomaly parameters |
| `POST` | `/api/ingestion/stop` | Stop streaming worker |
| `POST` | `/api/ingestion/batch` | Execute single high-speed bulk ingestion batch |
| `GET` | `/api/analytics/overview` | Executive KPI summary (P95/P99, latency, throughput, health score) |
| `GET` | `/api/analytics/timeseries` | Aggregated time-series buckets for dual-axis charts |
| `GET` | `/api/analytics/regions` | Regional distribution aggregation |
| `GET` | `/api/analytics/anomalies` | Detected statistical outliers and severity scores |
| `GET` | `/api/analytics/insights` | Automated actionable executive intelligence |
| `GET` | `/api/pipelines/presets` | Pre-built advanced MongoDB aggregation pipeline definitions |
| `POST` | `/api/pipelines/execute` | Execute arbitrary user-defined pipeline JSON with execution metrics |
| `GET` | `/api/data/documents` | Browse documents with pagination and multi-field filters |
| `GET` | `/api/export/csv` | Download dataset as CSV |
| `GET` | `/api/export/json` | Export dataset as sanitized JSON |
| `GET` | `/api/export/report` | Download executive analytical brief in Markdown |
| `WS` | `/ws/telemetry` | WebSocket stream emitting 1-second telemetry pulses |

---

## 📂 Project Structure

```
vortex-mongo-bigdata/
├── backend/
│   ├── app.py                # FastAPI gateway, REST endpoints & WebSocket server
│   ├── config.py             # Database settings, collection names & threshold limits
│   ├── database.py           # Resilient connection manager (Live Mongo & Embedded fallback)
│   ├── models.py             # Pydantic data schemas & document definitions
│   ├── data_generator.py     # Multi-tenant physical telemetry & anomaly generator
│   ├── ingestion.py          # Asynchronous micro-batch & bulk ingestion pipeline
│   ├── pipelines.py          # MongoDB Aggregation pipelines (Rollups, Facets, Buckets)
│   ├── analytics.py          # Z-Score outlier detection & automated heuristics engine
│   └── export_service.py     # CSV, JSON, and Markdown report export service
├── frontend/
│   ├── index.html            # Obsidian Cyber-Spatial UI dashboard
│   ├── css/
│   │   └── vortex-cyber.css  # Glassmorphism, bioluminescent glows & tactical HUD styling
│   └── js/
│       ├── app.js            # Main dashboard controller & reactive state manager
│       ├── visualizer.js     # Canvas particle physics & Chart.js dynamic visualizers
│       ├── pipeline_builder.js # Interactive Aggregation Studio workbench
│       ├── audio_synth.js    # Web Audio API procedural cyber sound synthesizer
│       └── websocket_client.js # Bidirectional WebSocket stream handler
├── data_samples/             # Sample Big Data JSON records (IoT & Cyber Security)
│   ├── iot_fleet_telemetry.json
│   └── cyber_security_events.json
├── scripts/
│   ├── seed_bigdata.py       # CLI tool to bulk seed 10k to 500k documents
│   └── run_benchmark.py      # MongoDB aggregation performance benchmark
├── tests/
│   └── test_pipelines.py     # Automated test suite
├── docker-compose.yml        # Docker compose configuration for MongoDB + Mongo Express
├── requirements.txt          # Python dependencies
├── run.bat                   # 1-click Windows startup batch script
├── start.ps1                 # 1-click PowerShell startup script
└── README.md                 # Complete documentation
```

---

## 🛡️ License
MIT License. Built for enterprise Big Data processing with MongoDB.
