@echo off
title AE Baccarat Workbench - Web Dashboard
cd /d "%~dp0"

echo =======================================================
echo   KHOI DONG AE BACCARAT WORKBENCH - WEB DASHBOARD
echo =======================================================
echo.
echo May chu dang khoi chay tai cong 8000...
echo Trinh duyet se tu dong mo trang web Dashboard.
echo.

python -m ae_baccarat_workbench.web.server

if errorlevel 1 (
    echo.
    echo =======================================================
    echo May chu Web da dung lai voi ma loi %errorlevel%.
    echo =======================================================
    pause
)
