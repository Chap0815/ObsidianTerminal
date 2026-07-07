@echo off
REM ═══════════════════════════════════════════════════════════════════
REM   OBSIDIAN TRADING TERMINAL — Launcher starten (ohne Konsole)
REM ═══════════════════════════════════════════════════════════════════
REM Doppelklick auf diese Datei startet den Launcher mit pythonw.exe.
REM Es erscheint KEIN schwarzes Konsolenfenster mehr — und es gibt
REM nichts mehr, das man versehentlich schließen und damit die Bots
REM killen könnte. Diese .bat schließt sich nach dem Start sofort selbst.

cd /d "%~dp0"

REM ── 1) Eingebettetes pythonw.exe bevorzugen (portable Installation) ──
if exist "%~dp0python\pythonw.exe" (
    start "" "%~dp0python\pythonw.exe" "%~dp0launcher.pyw"
    goto :eof
)

REM ── 2) py-Launcher: dessen pythonw über das -w-Flag nutzen ──
where pyw >nul 2>nul
if %errorlevel%==0 (
    start "" pyw "%~dp0launcher.pyw"
    goto :eof
)

REM ── 3) pythonw.exe aus dem PATH ──
where pythonw >nul 2>nul
if %errorlevel%==0 (
    start "" pythonw "%~dp0launcher.pyw"
    goto :eof
)

REM ── 4) Letzter Fallback: py-Launcher (kann kurz ein Fenster zeigen) ──
where py >nul 2>nul
if %errorlevel%==0 (
    start "" py "%~dp0launcher.pyw"
    goto :eof
)

REM ── Nichts gefunden: einmalig Hinweis zeigen (hier MIT Konsole) ──
echo.
echo   [FEHLER] pythonw.exe wurde nicht gefunden.
echo   Pruefe deine Python-Installation oder starte launcher.pyw
echo   per Doppelklick im Explorer.
echo.
pause
