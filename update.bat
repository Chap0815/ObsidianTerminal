@echo off
REM ================================================================
REM   OBSIDIAN TRADING TERMINAL - Update aus dem offiziellen Git-Repository
REM ================================================================
setlocal enableextensions
set PYTHONPATH=
set PYTHONHOME=
set PYTHONNOUSERSITE=1
cd /d "%~dp0"

py -3.12 -c "import pathlib, portalocker, psutil, sys; root=pathlib.Path(sys.argv[1]).resolve(); exe=pathlib.Path(sys.executable).resolve(); raise SystemExit(1 if sys.version_info[:2] != (3, 12) or exe == root or root in exe.parents else 0)" "%~dp0." >nul 2>nul
if %errorlevel%==0 (
    py -3.12 "%~dp0tools\update_from_git.py" %*
    goto :end
)

where python >nul 2>nul
if %errorlevel%==0 (
    python -c "import pathlib, portalocker, psutil, sys; root=pathlib.Path(sys.argv[1]).resolve(); exe=pathlib.Path(sys.executable).resolve(); raise SystemExit(1 if sys.version_info[:2] != (3, 12) or exe == root or root in exe.parents else 0)" "%~dp0." >nul 2>nul
)
if %errorlevel%==0 (
    python "%~dp0tools\update_from_git.py" %*
    goto :end
)

echo.
echo   [FEHLER] Kein geeignetes externes Update-Python gefunden.
echo   Starte Obsidian und verwende den gelben Update-Button. Dieser kopiert
echo   die gebuendelte Runtime vor dem Update in ein verifiziertes Temp-Verzeichnis.
echo.
set RC=2
goto :done

:end
set RC=%ERRORLEVEL%
:done
if not "%1"=="--quiet" pause
exit /b %RC%
