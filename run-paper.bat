@echo off
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  py -3 -m venv .venv
  if errorlevel 1 goto fail
)
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 goto fail
.venv\Scripts\python.exe -m radar paper --config config.json --data-dir data-paper-v61
if errorlevel 1 goto fail
pause
exit /b 0
:fail
echo Startup failed. Read the error above; no real orders were submitted.
pause
exit /b 1
