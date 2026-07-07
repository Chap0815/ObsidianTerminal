@echo off
setlocal
cd /d "%~dp0.."

echo ================ PRE-START CHECK ================
py -3 -m core.pre_start_check
echo.

echo ================ RUNTIME STATUS ================
powershell -NoProfile -Command ^
  "$ErrorActionPreference='SilentlyContinue';" ^
  "$files=Get-ChildItem logs -Recurse -Filter runtime_status.json;" ^
  "if(-not $files){ 'no runtime_status.json files found'; exit 0 }" ^
  "$rows=foreach($f in $files){" ^
  "  $j=Get-Content -Raw $f.FullName | ConvertFrom-Json;" ^
  "  [pscustomobject]@{Bot=$j.bot;Status=$j.status;Mode=($(if($j.simulation){'SIM'}else{'LIVE'}));Pid=$j.pid;Run=$j.run_id;Build=$j.build_id;Updated=$j.updated_at}" ^
  "};" ^
  "$rows | Format-Table -Auto"
echo.

echo ================ PYTHON PROCESSES ================
powershell -NoProfile -Command ^
  "$p=Get-Process py,python,pythonw -ErrorAction SilentlyContinue;" ^
  "if($p){ $p | Select-Object Id,ProcessName,@{n='MB';e={[math]::Round($_.WorkingSet/1MB,0)}},StartTime | Format-Table -Auto }" ^
  "else { 'no python process active' }"
echo.
pause
