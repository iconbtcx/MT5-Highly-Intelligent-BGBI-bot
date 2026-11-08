@echo off
setlocal
cd /d "%~dp0"
set "PY=%~dp0.kronos-venv\Scripts\python.exe"
if not exist "%PY%" (
  echo Run setup_kronos_bridge.bat first.
  pause
  exit /b 1
)
start "BGBI Crypto Flow - 5 pairs" cmd /k ""%PY%" "%~dp0BGBI-Crypto-Live-Flow-Bridge.py""
start "BGBI USD Hybrid - FX and DE40 context" cmd /k ""%PY%" "%~dp0BGBI-USD-Live-Hybrid-Bridge-V1.1.py""
start "BGBI Kronos Mini - 10 symbols" cmd /k ""%PY%" "%~dp0BGBI-Kronos-Mini-Bridge.py""
echo Started the three bridge windows. Keep them open while the EA is running.
