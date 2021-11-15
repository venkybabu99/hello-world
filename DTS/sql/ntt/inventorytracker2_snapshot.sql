-----------------------------------------
-- InventoryTrackerUpdate.py
-----------------------------------------
USE [master]
GO

CREATE DATABASE [inventorytracker2_python]
 ON  
( NAME = N'inventorytracker2_data', FILENAME = N'Z:\data\Data1\inventorytracker2_inventorytracker2_data_python.mdf' )
AS SNAPSHOT OF inventorytracker2
GO
