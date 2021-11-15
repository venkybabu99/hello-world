---------------------------------
-- AntiFraud.py
---------------------------------
USE [master]
GO

CREATE DATABASE [Eagle_python]
 ON
( NAME = N'eagle', FILENAME = N'Z:\data\Data5\Eagle_eagle_python.mdf' ),
( NAME = N'eagle_xtra', FILENAME = N'Z:\data\Data3\Eagle_eagle_xtra_python.mdf' )
AS SNAPSHOT OF Eagle
GO
