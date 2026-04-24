@echo off
title Build AI Race Engineer EXE — OpMo eSports
cd /d "%~dp0"

echo ============================================
echo   AI Race Engineer — EXE Build
echo   OpMo eSports
echo ============================================
echo.

set PYTHON=py -3.12
%PYTHON% --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python 3.12 not found. Install it from python.org.
    pause & exit /b 1
)

echo [1/3] Installing build dependencies...
%PYTHON% -m pip install pyinstaller -q
%PYTHON% -m pip install -r requirements_engineer.txt -q
echo Done.
echo.

echo [2/3] Cleaning previous build...
if exist build rmdir /s /q build
if exist dist\AIRaceEngineer.exe del /q dist\AIRaceEngineer.exe
echo Done.
echo.

echo [3/3] Building EXE (this takes 2-5 minutes)...
%PYTHON% -m PyInstaller engineer.spec --noconfirm
echo.

if exist dist\AIRaceEngineer.exe (
    echo ============================================
    echo   BUILD SUCCESSFUL
    echo   Output: dist\AIRaceEngineer.exe
    echo ============================================
    echo.
    echo You can copy AIRaceEngineer.exe anywhere and run it.
    echo On first run, place engineer_config.json and race_plan.json
    echo in the same folder as the EXE.
) else (
    echo ============================================
    echo   BUILD FAILED — check errors above
    echo ============================================
)

echo.
pause
