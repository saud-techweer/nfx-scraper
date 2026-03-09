@echo off
title NFX URL Collector (SERVER)
echo ============================================================
echo   NFX Signal - URL Collector (SERVER - auto-restarts)
echo ============================================================
echo.

:loop
python "%~dp0collect_urls.py" --server

if %ERRORLEVEL% EQU 0 (
    echo.
    echo   DONE! All URLs collected.
    pause
    exit /b 0
)

echo.
echo   Crashed! Restarting in 5 seconds...
echo   Press Ctrl+C to stop.
echo.
timeout /t 5 /nobreak
goto loop
