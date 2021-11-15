---------------------------------
-- AntiFraud.py
---------------------------------
USE [master]
GO

CREATE DATABASE [QCPro_python]
 ON
( NAME = N'qcpro_Data', FILENAME = N'Z:\data\Data5\QCPro_qcpro_Data_python.mdf' ),
( NAME = N'qcpro_1_Data', FILENAME = N'Z:\data\Data1\QCPro_qcpro_1_Data_python.mdf' ),
( NAME = N'qcpro_Data2', FILENAME = N'Z:\data\Data2\QCPro_qcpro_Data2_python.mdf' )
AS SNAPSHOT OF QCPro
GO
