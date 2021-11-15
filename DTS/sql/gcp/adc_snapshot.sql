-------------------------------------------------
--ADCExtract.py
-------------------------------------------------
USE [master]
GO

CREATE DATABASE [ADC_python]
 ON
( NAME = N'qcpro', FILENAME = N'D:\MSSQL\DATA\ADC_qcpro_python.mdf' ),
( NAME = N'ADCData', FILENAME = N'D:\MSSQL\DATA\ADC_ADCData_python.mdf'),
( NAME = N'ADCData2', FILENAME = N'D:\MSSQL\DATA\ADC_ADCData2_python.mdf' )
AS SNAPSHOT OF ADC
GO
