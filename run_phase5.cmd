@echo off
setlocal
pushd "%~dp0"

if not exist ".env.phase3" (
    echo Missing .env.phase3. Keep/reuse the Phase 3 warehouse credentials.
    popd
    exit /b 1
)
if not exist ".env.phase5" (
    echo Missing .env.phase5. Run: python tools\init_phase5_env.py
    popd
    exit /b 1
)
docker info >nul 2>&1
if errorlevel 1 (
    echo Docker is not ready. Start Docker Desktop first.
    popd
    exit /b 1
)
if not exist "reports" mkdir reports
if not exist "logs" mkdir logs
if not exist "data\changes" mkdir data\changes

set "DC=docker compose --env-file .env.phase3 --env-file .env.phase5 -f compose.phase3.yml -f compose.phase5.yml"

if /I "%~1"=="--build" goto BUILD
if /I "%~1"=="--start" goto START
if /I "%~1"=="--self-test" goto SELFTEST
if /I "%~1"=="--password" goto PASSWORD
if /I "%~1"=="--trigger" goto TRIGGER
if /I "%~1"=="--status" goto STATUS
if /I "%~1"=="--stop" goto STOP

echo Usage:
echo   run_phase5.cmd --build
echo   run_phase5.cmd --start
echo   run_phase5.cmd --self-test
echo   run_phase5.cmd --password
echo   run_phase5.cmd --trigger [batch_id]
echo   run_phase5.cmd --status
echo   run_phase5.cmd --stop
popd
exit /b 2

:BUILD
%DC% build airflow-api-server
set "RC=%ERRORLEVEL%"
goto END

:START
%DC% up -d --wait warehouse airflow-db
if errorlevel 1 (
    set "RC=%ERRORLEVEL%"
    goto END
)
%DC% run --rm --no-deps airflow-volume-init
if errorlevel 1 (
    set "RC=%ERRORLEVEL%"
    goto END
)
%DC% run --rm airflow-init
if errorlevel 1 (
    set "RC=%ERRORLEVEL%"
    goto END
)
%DC% up -d --wait airflow-api-server airflow-scheduler airflow-dag-processor
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" echo Airflow is ready at http://localhost:8080
if "%RC%"=="0" echo Run: run_phase5.cmd --self-test
goto END

:SELFTEST
%DC% run --rm --no-deps airflow-scheduler python /workspace/src/orchestration/run_phase5_selftest.py
if errorlevel 1 (
    set "RC=%ERRORLEVEL%"
    goto END
)
%DC% run --rm --no-deps airflow-scheduler /opt/airflow/project-venv/bin/python -c "import psycopg, pyarrow; print('project runtime imports OK')"
if errorlevel 1 (
    set "RC=%ERRORLEVEL%"
    goto END
)
%DC% run --rm --no-deps airflow-scheduler /opt/airflow/project-venv/bin/dbt --version
set "RC=%ERRORLEVEL%"
goto END

:PASSWORD
%DC% exec -T airflow-api-server cat /opt/airflow/state/simple_auth_manager_passwords.json.generated
set "RC=%ERRORLEVEL%"
goto END

:TRIGGER
%DC% exec -T airflow-scheduler airflow dags unpause ecommerce_incremental_pipeline >nul
if errorlevel 1 (
    set "RC=%ERRORLEVEL%"
    goto END
)
if "%~2"=="" (
    %DC% exec -T airflow-scheduler airflow dags trigger ecommerce_incremental_pipeline
) else (
    %DC% exec -T airflow-scheduler airflow dags trigger -c "{\"batch_id\":\"%~2\"}" ecommerce_incremental_pipeline
)
set "RC=%ERRORLEVEL%"
goto END

:STATUS
%DC% ps
set "RC=%ERRORLEVEL%"
goto END

:STOP
%DC% stop airflow-api-server airflow-scheduler airflow-dag-processor airflow-db
set "RC=%ERRORLEVEL%"
goto END

:END
if not defined RC set "RC=0"
popd
exit /b %RC%
