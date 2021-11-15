--------------------------------------
-- DQMCExtract.py
--------------------------------------
USE [master]
GO

CREATE DATABASE [readme_python]
 ON
( NAME = N'readme', FILENAME = N'Z:\data\Data3\readme_readme_python.mdf' )
AS SNAPSHOT OF readme
GO
