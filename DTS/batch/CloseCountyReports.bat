@echo off
SET AppPy=CloseCountyReports
E:
cd \DTS
Scripts\python.exe App\%AppPy%.py
EXIT /B %ERRORLEVEL%