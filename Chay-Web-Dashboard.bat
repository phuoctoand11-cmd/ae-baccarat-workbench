@echo off
title AE Baccarat Workbench - Web Dashboard
cd /d "%~dp0"

echo =======================================================
echo   KHOI DONG AE BACCARAT WORKBENCH - WEB DASHBOARD
echo =======================================================
echo.
echo May chu dang khoi chay tai cong 8000...
echo.

"AE-Baccarat-Workbench.exe" --web

if errorlevel 1 (
    echo.
    echo May chu Web da dung lai voi ma loi %errorlevel%.
    pause
)
