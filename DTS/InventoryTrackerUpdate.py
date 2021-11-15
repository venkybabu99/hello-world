# Initial Author: Byron Centeno
# Date Written: May 2021
# Overview: 1. Rewrite InventoryTrackerUpdate SSIS Package to Python
#           2. This process
#               a. Executes 2 queries in diablo - Truncates and Count
#               b. Inserts data to TransClosedDate table in inventory
#                  tracker db
#               c. Updates Close List
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=inventorytracker2      I/O
# SQL Server=live-diablo-dev.infosolco.com
# SQL DB=Diablo                 I
##############################################################################
import time
import os
from configs.ConfigUtility import AppConfig
from configs import EnvironmentConfig
from database import Database
from utils import get_logger
from utils.EmailUtil import EmailUtil

log = get_logger('InventoryTrackerUpdate')

class InventoryTrackerUpdate:
    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.email_util = EmailUtil(log=log)
        self.dts_inv_instance = Database.connect_dts_inventorytracker2(app_name=self.app_name, log=log)

    def CopyTransClosedDate(self):
        LastClosedDate_Sql = "SELECT CONVERT(INT, REPLACE(CONVERT(CHAR(10), MAX([Date]), 121), '-', '')) AS LastClosedDt FROM CloseList"
        LastClosedDate = self.dts_inv_instance.fn_fetch(query=LastClosedDate_Sql, fetch='fetchval',commit=False,close_conn=False)

        Read_DiabloTransClosedDate_Sql = (""" 
                ;WITH TransClosedDate AS (
                        SELECT
                            tcd.CntyCd,
                            tcd.RecordingDt,
                            tcd.ClosedDt,
                            tcd.ClosedCd,
                            tcd.ClosedById,
                            tcd.UpdateTimeStamp
                        FROM tCommon.TransClosedDate tcd
                        WHERE tcd.ClosedDt
                            BETWEEN {0} AND
                                CONVERT(INT, REPLACE(CONVERT(CHAR(10),
                                    DATEADD(WEEK, 1, CONVERT(DATETIME,
                                        CONVERT(VARCHAR, {1})))
                                            , 121), '-', ''))
                    ), RecordCounts AS (
                        SELECT
                            t.CntyCd,
                            tcd.RecordingDt,
                            COUNT(*) AS TotalRecords,
                            SUM(CASE
                                WHEN EditTimestamp IS NULL
                                    THEN 1
                                    ELSE 0 END) AS BypassedRecords
                        FROM TransClosedDate tcd
                        INNER JOIN tTrans.Trans t WITH (FORCESEEK, INDEX = 1)
                        ON tcd.CntyCd = t.CntyCd
                        AND t.RecordingDt
                            BETWEEN CONVERT(INT, CONVERT(VARCHAR,
                                        tcd.RecordingDt) + '01') AND
                                    CONVERT(INT, CONVERT(VARCHAR,
                                        tcd.RecordingDt) + '31')
                        GROUP BY t.CntyCd, tcd.RecordingDt
                    )
                    SELECT
                        tcd.CntyCd,
                        tcd.RecordingDt,
                        tcd.ClosedDt,
                        tcd.ClosedCd,
                        tcd.ClosedById,
                        tcd.UpdateTimeStamp,
                        rc.TotalRecords,
                        rc.BypassedRecords
                    FROM TransClosedDate tcd
                    INNER JOIN RecordCounts rc
                    ON tcd.CntyCd = rc.CntyCd
                    AND tcd.RecordingDt = rc.RecordingDt""").format(LastClosedDate,LastClosedDate)

        #Connect to diablo
        dts_diablo_instance = Database.connect_diablo(app_name=self.app_name, log=log)
        #Read diablo TransClosedDate and populate Dataframe
        df = dts_diablo_instance.fn_populate_dataframe(query=Read_DiabloTransClosedDate_Sql)
        dts_diablo_instance.fn_close()

        dest_table_name = '[dbo].[TransClosedDate]'
        #Write dataframe to destination table in DTS
        self.dts_inv_instance.fn_load_dataframe_to_table(dest_table_name=dest_table_name, dataframe=df, truncate_table = True, commit = True, close_conn = False)
        
        # Update Close List
        query = 'exec UpdateCloseList'
        self.dts_inv_instance.fn_execute(query=query, commit=True, close_conn=True)

    def main(self):
        start = time.perf_counter() 
        rc = 0        
        try:
            self.CopyTransClosedDate()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)  

obj = InventoryTrackerUpdate()
obj.main()