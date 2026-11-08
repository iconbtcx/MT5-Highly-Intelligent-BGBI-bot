@echo off
setlocal
cd /d "%~dp0"
if not exist "%~dp0.kronos-venv\Scripts\python.exe" (
  echo Run setup_kronos_bridge.bat first.
  pause
  exit /b 1
)
"%~dp0.kronos-venv\Scripts\python.exe" "%~dp0BGBI-USD-Live-Hybrid-Bridge-V1.1.py"
pause
