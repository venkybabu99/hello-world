@echo off
SET AppPy=CRECounts
E:
cd \DTS
Scripts\python.exe App\%AppPy%.py
EXIT /B %ERRORLEVEL%