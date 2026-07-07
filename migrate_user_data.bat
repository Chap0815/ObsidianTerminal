@echo off
REM ============================================================
REM migrate_user_data.bat — moves existing runtime data into new
REM sub-folder layout. Run ONCE after extracting the new project.
REM ============================================================

setlocal enabledelayedexpansion
cd /d "%~dp0"

echo.
echo ============================================================
echo  Migrating user data to new sub-folder layout
echo ============================================================
echo.

REM ── Ensure target folders exist ──
if not exist "data\" mkdir data
if not exist "logs\A\" mkdir "logs\A"
if not exist "logs\B\" mkdir "logs\B"
if not exist "logs\F\" mkdir "logs\F"
if not exist "docs\" mkdir docs

REM ── Move database files ──
echo [1/5] Moving database files to data/...
for %%F in (trading_bot.db trading_bot.db-shm trading_bot.db-wal) do (
    if exist "%%F" (
        if not exist "data\%%F" (
            move /Y "%%F" "data\%%F" >nul && echo   moved %%F
        ) else (
            echo   WARN: data\%%F already exists, skipping %%F
        )
    )
)

REM ── Move state files ──
echo [2/5] Moving state files to data/...
for %%F in (symbol_first_seen.json) do (
    if exist "%%F" (
        if not exist "data\%%F" (
            move /Y "%%F" "data\%%F" >nul && echo   moved %%F
        ) else (
            echo   WARN: data\%%F exists, keeping new version, deleting old
            del "%%F"
        )
    )
)

REM Delete old indicator_failures.json - schema changed
if exist "indicator_failures.json" (
    echo   deleting old indicator_failures.json (schema changed)
    del "indicator_failures.json"
)

REM ── Move log folders ──
echo [3/5] Moving log folders to logs/A,B,F/...
for %%P in (A B F) do (
    if exist "logs%%P\" (
        echo   merging logs%%P\ into logs\%%P\...
        REM Use xcopy to merge contents, then remove old folder
        xcopy /E /Y /Q "logs%%P\*" "logs\%%P\" >nul 2>&1
        rmdir /S /Q "logs%%P" 2>nul && echo     done
    )
)

REM ── Move docs ──
echo [4/5] Moving documentation to docs/...
for %%F in (_README.md _USER_GUIDE.txt _INSTALL_GUIDE.txt STRATEGY.md THREADING.md LICENSE.txt) do (
    if exist "%%F" (
        if not exist "docs\%%F" (
            move /Y "%%F" "docs\%%F" >nul && echo   moved %%F
        ) else (
            echo   WARN: docs\%%F exists, skipping
        )
    )
)

REM ── Verify the setup ──
echo [5/5] Verifying...
if exist ".env" (
    echo   OK: .env present
) else (
    echo   WARN: .env missing — bot will not be able to connect to exchange!
)
if exist "bot_config.json" (
    echo   OK: bot_config.json present
)
if exist "data\trading_bot.db" (
    echo   OK: database in data\
) else (
    echo   INFO: no database yet — a fresh one will be created on first start
)

echo.
echo ============================================================
echo  Migration done. You can now start the launcher:
echo    launcher.pyw
echo ============================================================
echo.
pause
