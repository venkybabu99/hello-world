--------------------
-- AntiFraud.py
--------------------
USE [master]
GO

CREATE DATABASE [AntiFraud_python]
 ON
( NAME = N'AntiFraud', FILENAME = N'Z:\data\Data4\AntiFraud_AntiFraud_python.mdf' )
 AS SNAPSHOT OF AntiFraud
GO
