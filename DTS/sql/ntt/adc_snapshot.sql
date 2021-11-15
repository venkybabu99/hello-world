-------------------------------------------------
--ADCExtract.py
-------------------------------------------------
USE [master]
GO

CREATE DATABASE [ADC_python]
 ON
( NAME = N'qcpro', FILENAME = N'Z:\data\Data1\ADC_qcpro_python.mdf' ),
( NAME = N'ADCData', FILENAME = N'Z:\data\Data5\ADC_ADCData_python.mdf' ),
( NAME = N'ADCData2', FILENAME = N'Z:\data\Data5\ADC_ADCData2_python.mdf' )
AS SNAPSHOT OF ADC
GO
