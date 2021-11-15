--------------------------------
-- CountyEditReject.py
--------------------------------
USE [master]
GO

CREATE DATABASE [pvcstracker_python]
 ON
( NAME = N'pvcstracker_Data', FILENAME = N'D:\MSSQL\DATA\pvcstracker_pvcstracker_Data_python.mdf' )
AS SNAPSHOT OF pvcstracker
GO

