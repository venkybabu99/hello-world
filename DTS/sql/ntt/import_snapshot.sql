--------------------
-- DQImport.py
-----------------------
USE [master]
GO

CREATE DATABASE [import_python]
  ON  
( NAME = N'import', FILENAME = N'Z:\data\Data4\import_import_python.mdf' )
 AS SNAPSHOT OF import
GO
