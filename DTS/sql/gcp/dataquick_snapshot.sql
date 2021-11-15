--------------------------------------
-- DQMCExtract.py
--------------------------------------
USE [master]
GO

CREATE DATABASE [dataquick_python]
 ON
( NAME = N'dataquick', FILENAME = N'D:\MSSQL\DATA\dataquick_dataquick_python.mdf' )
AS SNAPSHOT OF dataquick
GO
