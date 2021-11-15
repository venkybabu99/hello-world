# Initial Author: Leo
# Date Written: April 2021
# Overview: 1.  Rewrite DQTrackingInfo SSIS Package to Python
#           2.  This process
#               a.  truncate table CurrentEditions
#               b.  copy Diablo Common.vTaxLoadControl to CurrentEditions
#               c.  Executes a Stored Procedure UpdateTrackingInfo
#
# History: To be completed for each change made after initial release
# Who: When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=dqweb      I/O
# SQL Server=live-diablo-dev.infosolco.com
# SQL DB=Diablo     I
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
from ftp import ftp
# ###################################

log = get_logger('DQTrackingInfo')

class DQTrackingInfo:

    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)        
        self.dts_dqweb_instance = Database.connect_dts_dqweb(app_name=self.app_name, log=log)


    def load_diablo_current_editions(self):
        
        log.info('Reading CurrentEditions Data...')
        pstart = time.perf_counter() 

        diablo_instance = Database.connect_diablo(app_name=self.app_name, log=log)
        query = self.app_config.get_parm_value(section=self.app_name,parm="get_diablo_currenteditions")
        df = diablo_instance.fn_populate_dataframe(query=query)

        rows_select = len(df)
        total_cols = len(df.columns)
        finish = time.perf_counter() 

        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows Read:{rows_select:,d}, # of Columns:{total_cols:,d}')

        dest_table_name = self.app_config.get_parm_value(section=self.app_name,parm="dest_fqn_table_name")
        log.info(f'Writing to...{dest_table_name}')
        pstart = time.perf_counter() 

        self.dts_dqweb_instance.fn_load_dataframe_to_table(dest_table_name=dest_table_name, dataframe=df, close_conn = False, truncate_table = True, commit = True) 

        query = f'SELECT COUNT(1) FROM {dest_table_name}'
        rows_written = self.dts_dqweb_instance.fn_fetch(query=query, fetch='fetchval', commit=False, close_conn=False)

        finish = time.perf_counter()             
        log.info(f'End Insert File, Elapsed: {round(finish-pstart, 2)} second(s), Rows Written:{rows_written:,d}')

        if rows_select != rows_written:
            error = f'Rows do not match, process failed...Read={rows_select:,d}, Written{rows_written:,d}'
            log.exception(error)
            raise ValueError(error)
            # exit(1)

        ''' Execute Stored Procedure '''
        sp_updatetrackinginf = self.app_config.get_parm_value(section=self.app_name,parm="sp_updatetrackinginfo")
        log.info(f'Executing SP, {sp_updatetrackinginf}')
        pstart = time.perf_counter() 
        self.dts_dqweb_instance.fn_fetch(query=sp_updatetrackinginf, fetch=None, commit=True, close_conn=False)    
        finish = time.perf_counter() 
        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s)')
        
        return


    def main(self):
        
        start = time.perf_counter()    
        rc = 0

        try:
            self.load_diablo_current_editions()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            ''' Close connection '''
            self.dts_dqweb_instance.fn_close()
            exit(rc)

            
obj = DQTrackingInfo()
obj.main()

