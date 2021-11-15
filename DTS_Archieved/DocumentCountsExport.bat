@echo off
SET AppPy=DocumentCountsExport
E:
cd \DTS
Scripts\python.exe App\%AppPy%.py
EXIT /B %ERRORLEVEL%