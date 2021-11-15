--------------------------------------
-- DQMCExtract.py
--------------------------------------
USE [master]
GO

CREATE DATABASE [readme_python]
 ON
( NAME = N'readme', FILENAME = N'D:\MSSQL\DATA\readme_readme_python.mdf' )
AS SNAPSHOT OF readme
GO
