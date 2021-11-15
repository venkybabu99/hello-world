--------------------------
-- CloseCountyReports.py
--------------------------
USE [master]
GO

CREATE DATABASE [autoreports_python]
 ON
( NAME = N'autoreports', FILENAME = N'D:\MSSQL\DATA\autoreports_autoreports_python.mdf' )
 AS SNAPSHOT OF autoreports
 GO

