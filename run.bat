@echo off
title VORTEX // MongoDB Big Data Engine
echo ========================================================
echo   VORTEX // MONGODB BIG DATA LAKEHOUSE & ANALYTICS
echo ========================================================
echo Starting FastAPI application server on http://127.0.0.1:8080 ...
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8080 --reload
pause
