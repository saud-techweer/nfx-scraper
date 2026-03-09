@echo off
echo Closing all Chrome instances...
taskkill /F /IM chrome.exe 2>nul
timeout /t 2 /nobreak >nul

echo Starting Chrome with remote debugging...
start "" "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="%~dp0chrome-debug"

echo.
echo Chrome started with debugging enabled on port 9222
echo You can now log in to https://signal.nfx.com/investor-lists
echo.
echo After logging in, run: python scraper.py
echo.
pause
