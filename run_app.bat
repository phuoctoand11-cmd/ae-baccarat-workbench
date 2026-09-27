@echo off
cd /d "%~dp0"
python -m ae_baccarat_workbench
if errorlevel 1 (
    echo.
    echo ========================================================
    echo Ung dung da dung lai voi ma loi %errorlevel%.
    echo ========================================================
    pause
)

