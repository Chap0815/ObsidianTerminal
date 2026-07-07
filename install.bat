@echo off
REM ================================================================
REM   OBSIDIAN TRADING TERMINAL - One-Click Installer (Doppelklick)
REM   Stellt bei Bedarf Python 3.12 bereit und installiert dann alles.
REM ================================================================
setlocal enableextensions
title Obsidian Trading Terminal - Installer
cd /d "%~dp0"

echo.
echo   Starte Installation...
echo.

REM 1) Python 3.12 bevorzugt via py-Launcher
py -3.12 --version >nul 2>nul
if %errorlevel%==0 (
    echo   [OK] Python 3.12 gefunden ^(py -3.12^).
    py -3.12 install.py
    goto :end
)

REM 2) Irgendein python im PATH? Version wird in install.py geprueft.
python --version >nul 2>nul
if %errorlevel%==0 (
    echo   [OK] python im PATH gefunden.
    python install.py
    goto :end
)

REM 3) Kein Python: versuche Auto-Installation via winget.
echo   [INFO] Kein Python gefunden. Versuche Auto-Installation...
where winget >nul 2>nul
if %errorlevel%==0 (
    echo   [INFO] Installiere Python 3.12 via winget ^(kann etwas dauern^)...
    winget install -e --id Python.Python.3.12 --accept-source-agreements --accept-package-agreements
    py -3.12 --version >nul 2>nul
    if %errorlevel%==0 (
        echo   [OK] Python 3.12 installiert. Starte Installer erneut...
        py -3.12 install.py
        goto :end
    )
    echo.
    echo   [INFO] Python installiert. Bitte dieses Fenster SCHLIESSEN und
    echo          install.bat ERNEUT starten ^(damit der PATH aktualisiert ist^).
    echo.
    pause
    goto :end
)

REM 4) Kein winget: manueller Hinweis.
echo.
echo   [FEHLER] Weder Python noch winget gefunden.
echo   Bitte Python 3.12.10 manuell installieren:
echo     https://www.python.org/downloads/release/python-31210/
echo   WICHTIG: Beim Setup "Add Python to PATH" anhaken!
echo   Danach install.bat erneut starten.
echo.
pause

:end
endlocal
