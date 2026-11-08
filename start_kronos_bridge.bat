@echo off
setlocal
cd /d "%~dp0"
set "BGBI_KRONOS_PORT=8798"
rem Set these to the exact names shown in MT5 Market Watch if broker symbols have suffixes.
set "BGBI_KRONOS_SYMBOLS=btcusd,ethusd,bnbusd,solusd,xrpusd,eurusd,gbpusd,usdcad,usdjpy,de40"
rem If there are multiple terminals, set BGBI_MT5_PATH to the exact terminal64.exe path.
rem set "BGBI_MT5_PATH=C:\Program Files\MetaTrader 5\terminal64.exe"
set "BGBI_KRONOS_LOOKBACK=256"
set "BGBI_KRONOS_SAMPLE_COUNT=1"
set "BGBI_KRONOS_SPREAD_MULTIPLIER=2.0"
if not exist "%~dp0.kronos-venv\Scripts\python.exe" (
  echo Run setup_kronos_bridge.bat first.
  pause
  exit /b 1
)
"%~dp0.kronos-venv\Scripts\python.exe" "%~dp0BGBI-Kronos-Mini-Bridge.py"
pause
