@echo off
REM ================================================================
REM   OBSIDIAN TRADING TERMINAL - Update aus dem offiziellen Git-Repository
REM ================================================================
setlocal enableextensions
set PYTHONPATH=
set PYTHONHOME=
set PYTHONNOUSERSITE=1
cd /d "%~dp0"

if exist "%~dp0python\python.exe" (
    "%~dp0python\python.exe" "%~dp0tools\update_from_git.py" %*
    goto :end
)

if exist "%~dp0.venv\Scripts\python.exe" (
    "%~dp0.venv\Scripts\python.exe" "%~dp0tools\update_from_git.py" %*
    goto :end
)

py -3.12 --version >nul 2>nul
if %errorlevel%==0 (
    py -3.12 "%~dp0tools\update_from_git.py" %*
    goto :end
)

where python >nul 2>nul
if %errorlevel%==0 (
    python "%~dp0tools\update_from_git.py" %*
    goto :end
)

echo.
echo   [FEHLER] Kein Python gefunden. Starte zuerst install.bat oder nutze die Setup-EXE.
echo.

:end
set RC=%ERRORLEVEL%
if not "%1"=="--quiet" pause
exit /b %RC%
