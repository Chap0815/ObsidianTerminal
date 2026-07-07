@echo off
REM ================================================================
REM   OBSIDIAN TRADING TERMINAL - Launcher starten (ohne Konsole)
REM ================================================================
REM Doppelklick auf diese Datei startet den Launcher mit pythonw.exe.
REM Es erscheint kein schwarzes Konsolenfenster. Diese .bat schliesst
REM sich nach dem Start sofort selbst.

cd /d "%~dp0"

REM 1) Projekt-.venv bevorzugen (normale Installation)
if exist "%~dp0.venv\Scripts\pythonw.exe" (
    start "" "%~dp0.venv\Scripts\pythonw.exe" "%~dp0launcher.pyw"
    goto :eof
)

REM 2) Eingebettetes pythonw.exe bevorzugen (portable Installation)
if exist "%~dp0python\pythonw.exe" (
    start "" "%~dp0python\pythonw.exe" "%~dp0launcher.pyw"
    goto :eof
)

REM 3) pyw-Launcher nutzen
where pyw >nul 2>nul
if %errorlevel%==0 (
    start "" pyw "%~dp0launcher.pyw"
    goto :eof
)

REM 4) pythonw.exe aus dem PATH
where pythonw >nul 2>nul
if %errorlevel%==0 (
    start "" pythonw "%~dp0launcher.pyw"
    goto :eof
)

echo.
echo   [FEHLER] pythonw.exe wurde nicht gefunden.
echo   Pruefe die Installation oder starte OBSIDIAN.vbs.
echo.
pause
