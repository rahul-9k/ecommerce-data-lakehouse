@echo off
setlocal EnableDelayedExpansion
pushd "%~dp0"

if /I "%~1"=="--self-test" goto SELFTEST

if not exist ".env.phase6" (
    echo Missing .env.phase6. Run tools\init_phase6_env.py after AWS authentication works.
    popd
    exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env.phase6") do (
    if not "%%A"=="" set "%%A=%%B"
)
if "%PHASE6_S3_BUCKET%"=="" (
    echo Missing PHASE6_S3_BUCKET in .env.phase6
    popd
    exit /b 1
)
if "%PHASE6_AWS_REGION%"=="" (
    echo Missing PHASE6_AWS_REGION in .env.phase6
    popd
    exit /b 1
)
if "%PHASE6_S3_PREFIX%"=="" (
    echo Missing PHASE6_S3_PREFIX in .env.phase6
    popd
    exit /b 1
)

set "BASE=python src\cloud\s3_publish.py --bucket %PHASE6_S3_BUCKET% --region %PHASE6_AWS_REGION% --profile %PHASE6_AWS_PROFILE% --prefix %PHASE6_S3_PREFIX%"

if /I "%~1"=="--init-bucket" goto INIT
if /I "%~1"=="--dry-run" goto DRYRUN
if /I "%~1"=="--publish" goto PUBLISH
if /I "%~1"=="--verify-only" goto VERIFY

echo Usage:
echo   run_phase6.cmd --self-test
echo   run_phase6.cmd --init-bucket
echo   run_phase6.cmd --dry-run
echo   run_phase6.cmd --publish
echo   run_phase6.cmd --verify-only
set "RC=2"
goto END

:SELFTEST
if not exist "reports" mkdir reports
python src\cloud\run_phase6_selftest.py
set "RC=%ERRORLEVEL%"
goto END

:INIT
%BASE% --init-bucket
set "RC=%ERRORLEVEL%"
goto END

:DRYRUN
%BASE% --publish --dry-run
set "RC=%ERRORLEVEL%"
goto END

:PUBLISH
%BASE% --publish
set "RC=%ERRORLEVEL%"
goto END

:VERIFY
%BASE% --verify-only
set "RC=%ERRORLEVEL%"
goto END

:END
popd
exit /b %RC%
