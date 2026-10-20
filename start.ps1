# VORTEX // PowerShell Quickstart Script
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  VORTEX // MONGODB BIG DATA LAKEHOUSE & ANALYTICS      " -ForegroundColor White
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "Launching server on http://127.0.0.1:8080..." -ForegroundColor Green

python -m uvicorn backend.app:app --host 127.0.0.1 --port 8080 --reload
