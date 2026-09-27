@echo off
title AE Baccarat Workbench - Desktop App
cd /d "%~dp0"
if exist "AE-Baccarat-Workbench.exe" (
    start "" "AE-Baccarat-Workbench.exe"
) else if exist "dist\AE-Baccarat-Workbench\AE-Baccarat-Workbench.exe" (
    start "" "dist\AE-Baccarat-Workbench\AE-Baccarat-Workbench.exe"
) else (
    python -m ae_baccarat_workbench
)
