--------------------------
-- CloseCountyReports.py
--------------------------
USE [master]
GO

CREATE DATABASE [autoreports_python]
 ON
( NAME = N'autoreports', FILENAME = N'Z:\data\Data3\autoreports_autoreports_python.mdf' )
 AS SNAPSHOT OF autoreports
 GO

