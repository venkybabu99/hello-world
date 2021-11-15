SELECT TOP( 1000 )
       [ConfigurationFilter]
     , [PackagePath]
     , [ConfiguredValue]
     , [ConfiguredValueType]
FROM [global].[dbo].[SSISConfig]
WHERE ConfigurationFilter IN ('InventoryTrackerUpdate', 'DQTrackingInfo', 'DQMMExtract', 'DQMCExtract', 'DQImport', 'CountyEditReject', 'CloseCountyReports', 'AntiFraud', 'ADCExtract') ;
