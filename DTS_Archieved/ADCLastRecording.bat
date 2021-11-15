@echo off
SET AppPy=ADCLastRecording
E:
cd \DTS
Scripts\python.exe App\%AppPy%.py
EXIT /B %ERRORLEVEL%