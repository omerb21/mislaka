@echo off
echo Stopping Python processes...
taskkill /f /im python.exe /t >nul 2>&1
echo Processes stopped.
echo.
echo Running pension extraction system...
python simple_automatic_system.py
pause
