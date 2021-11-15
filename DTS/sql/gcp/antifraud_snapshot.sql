--------------------
-- AntiFraud.py
--------------------
USE [master]
GO

CREATE DATABASE [AntiFraud_python]
 ON
( NAME = N'AntiFraud', FILENAME = N'D:\MSSQL\DATA\AntiFraud_AntiFraud_python.mdf'  )
 AS SNAPSHOT OF AntiFraud
GO
