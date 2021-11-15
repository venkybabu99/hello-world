-------------------------------
-- DQTrackingInfo.py
-------------------------------
USE [master]
GO

CREATE DATABASE [dqweb_python]
 ON 
( NAME = N'dqweb', FILENAME = N'Z:\data\Data1\dqweb_dqweb_python.mdf' )
AS SNAPSHOT OF dqweb
GO
