@echo off
SET AppPy=MarketracExtract
E:
cd \DTS
Scripts\python.exe App\%AppPy%.py
EXIT /B %ERRORLEVEL%