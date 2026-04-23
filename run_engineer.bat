@echo off
title AI Race Engineer — OpMo eSports
cd /d "%~dp0"

where python >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python not found. Install from https://python.org
    echo Make sure to tick "Add Python to PATH" during install.
    pause
    exit /b 1
)

echo Installing/updating dependencies...
python -m pip install -r requirements_engineer.txt -q

echo Starting AI Race Engineer...
python ai_engineer.py

if %errorlevel% neq 0 (
    echo.
    echo Engineer exited with an error. Check the log above.
    pause
)
