--------------------
-- DQImport.py
-----------------------
USE [master]
GO

CREATE DATABASE [import_python]
  ON  
( NAME = N'import', FILENAME = N'D:\MSSQL\DATA\import_import_python.mdf'  )
 AS SNAPSHOT OF import
GO
