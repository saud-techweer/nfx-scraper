@echo off
title NFX URL Collector
echo ============================================================
echo   NFX Signal - URL Collector (auto-restarts on crash)
echo ============================================================
echo.

:loop
python "%~dp0collect_urls.py"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================================
    echo   DONE! All URLs collected successfully.
    echo ============================================================
    pause
    exit /b 0
)

echo.
echo   Script crashed! Restarting in 5 seconds...
echo   (URLs saved to disk - will resume from where it left off)
echo   Press Ctrl+C to stop.
echo.
timeout /t 5 /nobreak
goto loop
