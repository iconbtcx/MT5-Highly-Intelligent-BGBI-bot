@echo off
setlocal
cd /d "%~dp0"
set "VENV_DIR=%~dp0.kronos-venv"

echo ======================================================
echo BGBI Kronos setup - Python 3.12 64-bit
echo ======================================================

rem pandas 2.2.2 has a Windows x64 wheel for CPython 3.12.
rem Force a binary wheel so pip never tries to compile pandas with Visual Studio.
py -3.12 -c "import sys,struct; print('Python:',sys.version); print('Architecture:',struct.calcsize('P')*8,'bit'); raise SystemExit(0 if sys.version_info[:2] == (3,12) and struct.calcsize('P') == 8 else 1)"
if errorlevel 1 (
  echo.
  echo ERROR: Python 3.12 64-bit was not found.
  echo Install the Windows x86-64 Python 3.12 installer from:
  echo https://www.python.org/downloads/
  echo Then close and reopen Command Prompt and run this file again.
  pause
  exit /b 1
)

rem Recreate only this package's failed/incompatible virtual environment.
if exist "%VENV_DIR%" (
  echo Removing previous incomplete Kronos virtual environment...
  rmdir /s /q "%VENV_DIR%"
  if exist "%VENV_DIR%" (
    echo ERROR: Could not remove %VENV_DIR%
    echo Close any Python process using this environment and retry.
    pause
    exit /b 1
  )
)

py -3.12 -m venv "%VENV_DIR%"
if errorlevel 1 (
  echo ERROR: Could not create the Python 3.12 virtual environment.
  pause
  exit /b 1
)

"%VENV_DIR%\Scripts\python.exe" -m pip install --upgrade pip setuptools wheel
if errorlevel 1 goto :failed

rem Install pandas from a prebuilt Windows wheel only; do not compile from source.
"%VENV_DIR%\Scripts\python.exe" -m pip install --only-binary=:all: "pandas==2.2.2"
if errorlevel 1 goto :failed

rem Install the remaining pinned/project dependencies.
"%VENV_DIR%\Scripts\python.exe" -m pip install -r "%~dp0requirements-kronos.txt"
if errorlevel 1 goto :failed

rem Verify all critical imports in the exact environment used by the bridge.
"%VENV_DIR%\Scripts\python.exe" -c "import sys,struct,numpy,pandas,torch,MetaTrader5 as mt5; print('Python:',sys.version.split()[0]); print('Architecture:',struct.calcsize('P')*8,'bit'); print('NumPy:',numpy.__version__); print('pandas:',pandas.__version__); print('PyTorch:',torch.__version__); print('MetaTrader5:',mt5.__version__)"
if errorlevel 1 goto :failed

echo.
echo SETUP COMPLETE.
echo Next: open MT5, then run start_kronos_bridge.bat.
pause
exit /b 0

:failed
echo.
echo SETUP FAILED. The error above identifies the package that failed.
echo Do not install Visual Studio just to work around a pandas wheel error.
pause
exit /b 1
