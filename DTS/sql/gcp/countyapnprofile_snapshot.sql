----------------------------------
-- DQMMExtract.py
----------------------------------
USE [master]
GO

CREATE DATABASE [CountyApnProfile_python]
 ON 
( NAME = N'CountyApnProfile', FILENAME = N'D:\MSSQL\DATA\CountyApnProfile_CountyApnProfile_python.mdf' )
 AS SNAPSHOT OF CountyApnProfile
GO
