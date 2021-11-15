@echo off
SET DTS_Environment=uat
E:
cd \DTS
Scripts\python.exe App\%1.py
EXIT /B %ERRORLEVEL%