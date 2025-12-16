@echo off
REM ============================================================================
REM Cluster Testing Suite - Windows Batch Script
REM ============================================================================
REM Usage: test_cluster.bat [test_name] [options]
REM
REM Tests:
REM   all         - Run all tests (default)
REM   load        - Load test with 100k entries
REM   consistency - Consistency verification
REM   chaos       - Chaos testing
REM   quick       - Quick smoke test
REM ============================================================================

setlocal enabledelayedexpansion

set BASE_PORT=8081
set NUM_ENTRIES=100000
set TEST=%1

if "%TEST%"=="" set TEST=all

echo ============================================================
echo RAFT CLUSTER TESTING SUITE
echo ============================================================
echo.

REM Check if cluster is running
echo Checking cluster status...
curl -s http://localhost:%BASE_PORT%/health >nul 2>&1
if errorlevel 1 (
    echo ERROR: Cluster not running!
    echo Start with: docker-compose -f deployments/docker/docker-compose.yml up -d
    exit /b 1
)
echo Cluster is running.
echo.

REM Find leader
for /L %%i in (0,1,4) do (
    set /a PORT=%BASE_PORT%+%%i
    for /f "tokens=*" %%j in ('curl -s http://localhost:!PORT!/cluster/info 2^>nul ^| findstr /C:"Leader"') do (
        echo Found leader on port !PORT!
        set LEADER_PORT=!PORT!
        goto :found_leader
    )
)
echo ERROR: No leader found!
exit /b 1

:found_leader
echo.

if "%TEST%"=="quick" goto :test_quick
if "%TEST%"=="load" goto :test_load
if "%TEST%"=="consistency" goto :test_consistency
if "%TEST%"=="chaos" goto :test_chaos
if "%TEST%"=="all" goto :test_all

echo Unknown test: %TEST%
exit /b 1

REM ============================================================================
REM Quick Smoke Test
REM ============================================================================
:test_quick
echo ============================================================
echo QUICK SMOKE TEST
echo ============================================================

echo Testing PUT...
curl -s -X PUT http://localhost:%LEADER_PORT%/kv/test-key -d "test-value" >nul
if errorlevel 1 (
    echo FAIL: PUT failed
    exit /b 1
)
echo PUT: OK

echo Testing GET...
curl -s http://localhost:%LEADER_PORT%/kv/test-key | findstr "test-value" >nul
if errorlevel 1 (
    echo FAIL: GET failed
    exit /b 1
)
echo GET: OK

echo Testing DELETE...
curl -s -X DELETE http://localhost:%LEADER_PORT%/kv/test-key >nul
if errorlevel 1 (
    echo FAIL: DELETE failed
    exit /b 1
)
echo DELETE: OK

echo.
echo [PASS] Quick Smoke Test
goto :end

REM ============================================================================
REM Load Test
REM ============================================================================
:test_load
echo ============================================================
echo LOAD TEST: %NUM_ENTRIES% entries
echo ============================================================

set SUCCESS=0
set ERRORS=0

echo Inserting entries...
for /L %%i in (1,1,%NUM_ENTRIES%) do (
    curl -s -X PUT http://localhost:%LEADER_PORT%/kv/load-key-%%i -d "load-value-%%i" >nul 2>&1
    if errorlevel 1 (
        set /a ERRORS+=1
    ) else (
        set /a SUCCESS+=1
    )

    REM Progress every 10000
    set /a MOD=%%i %% 10000
    if !MOD!==0 (
        echo   Progress: %%i / %NUM_ENTRIES% ^(success: !SUCCESS!, errors: !ERRORS!^)
    )
)

echo.
echo Results:
echo   Success: %SUCCESS%
echo   Errors: %ERRORS%

if %SUCCESS% GEQ 95000 (
    echo [PASS] Load Test
) else (
    echo [FAIL] Load Test
)
goto :end

REM ============================================================================
REM Consistency Test
REM ============================================================================
:test_consistency
echo ============================================================
echo CONSISTENCY TEST
echo ============================================================

set NUM_TEST=100

echo Inserting %NUM_TEST% test entries...
for /L %%i in (1,1,%NUM_TEST%) do (
    curl -s -X PUT http://localhost:%LEADER_PORT%/kv/consistency-key-%%i -d "consistency-value-%%i" >nul
)

echo Waiting for replication...
timeout /t 3 /nobreak >nul

echo Verifying across all nodes...
set CONSISTENT=1
for /L %%p in (0,1,4) do (
    set /a PORT=%BASE_PORT%+%%p
    set NODE_ERRORS=0

    for /L %%i in (1,1,10) do (
        curl -s http://localhost:!PORT!/kv/consistency-key-%%i 2>nul | findstr "consistency-value-%%i" >nul
        if errorlevel 1 (
            set /a NODE_ERRORS+=1
        )
    )

    if !NODE_ERRORS! GTR 0 (
        echo   Port !PORT!: !NODE_ERRORS! errors
        set CONSISTENT=0
    ) else (
        echo   Port !PORT!: OK
    )
)

if %CONSISTENT%==1 (
    echo [PASS] Consistency Test
) else (
    echo [FAIL] Consistency Test
)
goto :end

REM ============================================================================
REM Chaos Test
REM ============================================================================
:test_chaos
echo ============================================================
echo CHAOS TEST
echo ============================================================

echo Starting chaos test for 30 seconds...
set CHAOS_SUCCESS=0
set CHAOS_ERRORS=0

REM Simple chaos: stop and start nodes while doing operations
for /L %%s in (1,1,6) do (
    echo.
    echo --- Chaos round %%s ---

    REM Do some writes
    for /L %%i in (1,1,50) do (
        set /a KEY=%%s*100+%%i
        curl -s -X PUT http://localhost:%LEADER_PORT%/kv/chaos-key-!KEY! -d "chaos-value" >nul 2>&1
        if errorlevel 1 (
            set /a CHAOS_ERRORS+=1
        ) else (
            set /a CHAOS_SUCCESS+=1
        )
    )

    REM Stop a follower node (not leader)
    if %%s==2 (
        echo Stopping node2...
        docker stop raft-node2 >nul 2>&1
    )
    if %%s==4 (
        echo Starting node2...
        docker start raft-node2 >nul 2>&1
    )

    timeout /t 2 /nobreak >nul
)

REM Ensure all nodes are running
docker start raft-node2 >nul 2>&1
docker start raft-node3 >nul 2>&1
docker start raft-node4 >nul 2>&1
docker start raft-node5 >nul 2>&1

echo.
echo Results:
echo   Success: %CHAOS_SUCCESS%
echo   Errors: %CHAOS_ERRORS%

set /a TOTAL=%CHAOS_SUCCESS%+%CHAOS_ERRORS%
set /a RATE=%CHAOS_SUCCESS%*100/%TOTAL%

if %RATE% GEQ 50 (
    echo [PASS] Chaos Test ^(%RATE%%% success^)
) else (
    echo [FAIL] Chaos Test ^(%RATE%%% success^)
)
goto :end

REM ============================================================================
REM All Tests
REM ============================================================================
:test_all
echo Running all tests...
echo.

call :test_quick
echo.
call :test_consistency
echo.
call :test_chaos
echo.

echo ============================================================
echo ALL TESTS COMPLETE
echo ============================================================
goto :end

:end
echo.
echo Test complete.
endlocal
