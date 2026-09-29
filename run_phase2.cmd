@echo off
setlocal
pushd "%~dp0"
where docker >nul 2>&1
if errorlevel 1 (
    echo Docker was not found. Install Docker Desktop for Windows, then reopen VS Code.
    popd
    exit /b 1
)
docker compose version >nul 2>&1
if errorlevel 1 (
    echo Docker Compose was not found. Update or repair Docker Desktop.
    popd
    exit /b 1
)
docker info >nul 2>&1
if errorlevel 1 (
    echo Docker is installed but its engine is not ready. Start Docker Desktop in Linux-container mode.
    popd
    exit /b 1
)
docker compose -f compose.phase2.yml run --rm spark /workspace/src/run_phase2.py %*
set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%
