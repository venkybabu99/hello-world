# Initial Author: Anton
# Date Written: April 2021
# Overview: 1. Rewrite CloseCounty Report SSIS Package to Python
#           2. This process
#               a. pulls data from Diablo regarding closed counties,
#               b. loads them to a newly created table in the AutoReports
#                  Database in DTS
#               c. Executes a Stored Procedure to trigger a related report
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=AutoReports    I/O
# SQL Server=live-diablo-dev.infosolco.com
# SQL DB=Diablo         I
##############################################################################
import time
import os
#
from configs.ConfigUtility import AppConfig
from configs import EnvironmentConfig
from database import Database
from utils import get_logger
from utils.EmailUtil import EmailUtil
from utils.FileUtils import FileUtils
from utils.ZipUtils import ZipUtils
from vault import vault
# from ftp import ftp
# ###################################

log = get_logger('CloseCountyReports')

class CloseCountyReports:
    
    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)        
        self.dts_instance = Database.connect_dts_autoreports(app_name=self.app_name, log=log)

    
    def create_reports(self):
        ####################################################################
        # Connect to Diablo and extract the needed data.  Save in csv file #
        ####################################################################
        diablo_instance = Database.connect_diablo(app_name=self.app_name, log=log)
        log.info('Reading data from Diablo: {0}'.format(str(time.time())))
        records = diablo_instance.fn_populate_dataframe("EXEC [aTrans].[CloseCountyReportREM]")
        diablo_instance.fn_close()

        ##########################################################################
        # Now connect to DTS, create a new table and load the data in the csv file
        # into the new table.  Once loaded, execute a stored #
        # procedure to trigger the needed report(s)
        ##########################################################################
        log.info('Inserting data to DTS: {0}'.format(str(time.time())))
        self.dts_instance.fn_execute('''
            if object_id('CloseCountyReportREM') is not null
                drop table CloseCountyReportREM
            CREATE TABLE [dbo].[CloseCountyReportREM](
                [StateCd] [varchar](2) NULL,
                [CntyName] [varchar](30) NULL,
                [CntyCd] [varchar](5) NULL,
                [RecordingDt] [int] NULL,
                [01] [int] NULL,
                [02] [int] NULL,
                [03] [int] NULL,
                [04] [int] NULL,
                [05] [int] NULL,
                [06] [int] NULL,
                [07] [int] NULL,
                [08] [int] NULL,
                [09] [int] NULL,
                [10] [int] NULL,
                [11] [int] NULL,
                [12] [int] NULL,
                [13] [int] NULL,
                [14] [int] NULL,
                [15] [int] NULL,
                [16] [int] NULL,
                [17] [int] NULL,
                [18] [int] NULL,
                [19] [int] NULL,
                [20] [int] NULL,
                [21] [int] NULL,
                [22] [int] NULL,
                [23] [int] NULL,
                [24] [int] NULL,
                [25] [int] NULL,
                [26] [int] NULL,
                [27] [int] NULL,
                [28] [int] NULL,
                [29] [int] NULL,
                [30] [int] NULL,
                [31] [int] NULL,
                [Total] [int] NULL,
                [PrevClose] [int] NULL)
                ''', close_conn = False)
        # Insert data pulled from Diablo into DTS DB                
        self.dts_instance.fn_load_dataframe_to_table(dest_table_name = 'CloseCountyReportREM', dataframe = records, close_conn = False) 
        # Execute SQL Command to trigger reports
        log.info('Trigering DTS Reports: {0}'.format(str(time.time())))
        self.dts_instance.fn_execute(query = 
            "UPDATE s SET ForceRun = 1 FROM ReportGroups rg INNER JOIN ScheduleReportGroup srg ON rg.ReportGroupId = srg.ReportGroupId \
            INNER JOIN Schedules s ON srg.ScheduleId = s.ScheduleId \
            WHERE Name LIKE '%Close County Report (REM)%'", commit= True, close_conn=True
        )


    def main(self):

        start = time.perf_counter() 
        rc = 0

        try:
            self.create_reports()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)


obj = CloseCountyReports()
obj.main()