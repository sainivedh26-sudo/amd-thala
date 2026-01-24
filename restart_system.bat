@echo off
echo ========================================
echo Restarting ITSM Prediction System
echo ========================================
echo.

echo Stopping all Python processes...
taskkill /F /IM python.exe /T 2>nul
timeout /t 3 /nobreak >nul

echo.
echo Starting Integrated System (Flask + Jira + Slack + Kafka Consumer)...
echo.

cd /d "%~dp0"
start "ITSM Thala System" cmd /k "thala\Scripts\python.exe integrated_main.py"

echo.
echo ========================================
echo System started! Check the new window.
echo ========================================
echo.
echo Monitor logs:
echo   - Flask API: thala_prediction.log
echo   - Connectors: thala_integrated.log
echo.
echo Test endpoints:
echo   curl http://localhost:5000/health
echo.
pause

