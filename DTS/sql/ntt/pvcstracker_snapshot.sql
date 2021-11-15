--------------------------------
-- CountyEditReject.py
--------------------------------
USE [master]
GO

CREATE DATABASE [pvcstracker_python]
 ON
( NAME = N'pvcstracker_Data', FILENAME = N'Z:\data\Data3\pvcstracker_pvcstracker_Data_python.mdf' )
AS SNAPSHOT OF pvcstracker
GO

