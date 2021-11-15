-------------------------------
-- DQTrackingInfo.py
-------------------------------
USE [master]
GO

CREATE DATABASE [dqweb_python]
 ON 
( NAME = N'dqweb', FILENAME = N'D:\MSSQL\DATA\dqweb_dqweb_python.mdf' )
AS SNAPSHOT OF dqweb
GO
