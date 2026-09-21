@echo off
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  py -3 -m venv .venv
  if errorlevel 1 goto fail
)
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 goto fail

echo.
echo [Compression Radar 6.1.3] Checking Binance public REST/WebSocket connectivity...
.venv\Scripts\python.exe -m radar doctor
if errorlevel 1 goto fail

echo.
echo [Compression Radar 6.1.3] Starting live-market PAPER runtime...
.venv\Scripts\python.exe -m radar paper --config config.json --data-dir data-paper-v613
if errorlevel 1 goto fail
pause
exit /b 0

:fail
echo.
echo Startup failed. Read the error above. No real-money orders are supported or submitted.
pause
exit /b 1
