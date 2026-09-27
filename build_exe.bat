@echo off
echo ========================================================
echo   Building AE Baccarat Workbench Desktop .exe
echo ========================================================
cd /d "%~dp0"

echo [1/4] Kiem tra PyInstaller...
python -m PyInstaller --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [LOI] Chua cai PyInstaller trong Python hien tai.
    exit /b 1
)

echo [2/4] Dang dong goi bang PyInstaller...
python -m PyInstaller ae_baccarat_workbench.spec --clean -y --workpath ".artifact-build\work" --distpath "dist"
if %ERRORLEVEL% NEQ 0 (
    echo [LOI] Dong goi that bai!
    exit /b %ERRORLEVEL%
)

echo [3/4] Sao chep file khoi dong...
copy /Y "Chay-Web-Dashboard.bat" "dist\AE-Baccarat-Workbench\" >nul 2>&1
copy /Y "Chay-Desktop-App.bat" "dist\AE-Baccarat-Workbench\" >nul 2>&1

echo [4/4] Kiem tra du lieu cuc bo khong bi dong goi...
if exist "dist\AE-Baccarat-Workbench\config.local.json" (
    echo [LOI] Artifact co config.local.json.
    exit /b 1
)
if exist "dist\AE-Baccarat-Workbench\data\workbench.sqlite" (
    echo [LOI] Artifact co database SQLite.
    exit /b 1
)
if exist "dist\AE-Baccarat-Workbench\data\analytics.duckdb" (
    echo [LOI] Artifact co database DuckDB.
    exit /b 1
)

echo ========================================================
echo   DONG GOI THANH CONG!
echo   Thu muc ung dung: dist\AE-Baccarat-Workbench\
echo   File chay Desktop GUI: dist\AE-Baccarat-Workbench\AE-Baccarat-Workbench.exe
echo   File chay Web App:     dist\AE-Baccarat-Workbench\Chay-Web-Dashboard.bat
echo ========================================================
