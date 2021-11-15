# Initial Author: Anton
# Date Written: Sept 2021
# Overview: 1. Rewrite MarketracExtract SSIS Package to Python
#           2. This process pulls data from Diablo into the inventorytracker2
#              database
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=global
# SQL DB=inventorytracker2
# SQL Server=live-diablo-dev.infosolco.com
# SQL DB=Diablo
##############################################################################
import os
import time
#
from configs.ConfigUtility import AppConfig
from configs import EnvironmentConfig
from database import Database
from utils import get_logger
from utils.EmailUtil import EmailUtil
from utils.FileUtils import FileUtils
from utils.ZipUtils import ZipUtils
from vault import vault
from ftp import ftp
# ###################################

log = get_logger('MarketracExtract')

class MarketracExtract:

    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)        
        self.diablo_instance = Database.connect_diablo(app_name=self.app_name, log=log)
        self.dts_instance_global = Database.connect_dts_global(app_name=self.app_name, log=log)
        self.dts_instance_inventorytracker2 = Database.connect_dts_inventorytracker2(app_name=self.app_name, log=log)

    
    def copy_data(self):
        # Get NextClosedDt from DTS to be used in Diablo queries
        # Test value for date when starting = 20210529
        qry = '''SELECT top 1 [ConfiguredValue] as NextClosedDt
            FROM [global].[dbo].[SSISConfig]
            where ConfigurationFilter = 'MarketracExtract' and PackagePath like '%NextClosedDt%\''''
        NextClosedDate = self.dts_instance_global.fn_fetch(query=qry, fetch = 'fetchone', close_conn = True)[0]
        log.info(f"Min Recording Date={str(NextClosedDate)}")

        # Get data from Diablo into dataframes
        qry = f'EXEC aExtract.DtsMarketracExtract @ClosedDt = {str(NextClosedDate)}'
        log.info(f"Diablo Query={qry}")
        Marketrac_records = self.diablo_instance.fn_populate_dataframe(query=qry)
        self.diablo_instance.fn_close()

        # Load data to tables in DTS
        self.dts_instance_inventorytracker2.fn_load_dataframe_to_table(dest_table_name="MarketracStaging", dataframe=Marketrac_records, truncate_table = True, commit = True, close_conn = False)

        #Copy data from staging table to actual table
        qry = f'exec MarketracCopyFromStaging @ClosedDt = {str(NextClosedDate)}'
        self.dts_instance_inventorytracker2.fn_execute(query=qry, commit = True, close_conn = False)
        self.dts_instance_inventorytracker2.fn_execute(query="exec UpdateFormattedMarketrakExtractStatic", commit = True, close_conn = False)
        self.dts_instance_inventorytracker2.fn_execute(query="exec MarketracIncrementNextClosedDt", commit = True, close_conn = True)


    def main(self):

        start = time.perf_counter() 
        rc = 0        

        try:
            self.copy_data()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)  


obj = MarketracExtract()
obj.main()
