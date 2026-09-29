@echo off
setlocal
pushd "%~dp0"
if not exist ".env.phase3" (
    echo Missing .env.phase3. Run: python tools\init_phase3_env.py
    popd
    exit /b 1
)
docker info >nul 2>&1
if errorlevel 1 (
    echo Docker is not ready. Start Docker Desktop in Linux-container mode.
    popd
    exit /b 1
)
if not exist "reports" mkdir reports
if not exist "logs" mkdir logs
docker compose --env-file .env.phase3 -f compose.phase3.yml run --rm --no-deps warehouse-runner %*
set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%
