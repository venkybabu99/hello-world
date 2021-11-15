# Initial Author: Danny Skandrani
# Date Written: May 2021
# Overview: 1. Rewrite DocumentCountsExport Report SSIS Package to Python
#           2. This process
#               a. Executes 3 queries in diablo
#               b. Creates 3 seperate flat files with the output for each query
#               c. Pushes the 3 files to SFTP location
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=fulfillment-diablo-dev.infosolco.com
# SQL DB=DiabloSynonyms
##############################################################################
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import inspect
# ###################################
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

log = get_logger('DocumentCountsExport')

class DocumentCountsExport:

    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)
        self.localpath = self.app_config.get_parm_value(section=self.app_name,parm="localpath")
        self.file_util.CheckDirExists(folder=self.localpath, createdir=True)
        self.todayyyymmdd = str(datetime.datetime.now())[0:10].replace("-", '') 
        
        # Setup sftp credentials
        self.ftp_port = 22
        self.ftp_host = self.app_config.get_parm_value(section=self.app_name,parm="ftp_host")
        self.ftp_user = self.app_config.get_parm_value(section=self.app_name,parm="ftp_user")
        self.ftp_path = self.app_config.get_parm_value(section=self.app_name,parm="ftp_path")
        cred = self.vault_util.get_secret(self.ftp_user)
        self.ftp_pswd = cred['password'] 


    def DeedCatTyp(self):

        func = inspect.getframeinfo(inspect.currentframe()).function
        log.info(f"Func={func} extract starting...")
        start = time.perf_counter()

        dts_diablo_instance = Database.connect_diablo(app_name=self.app_name + ' DeedCatTyp', log=log)
        #Query for DeedCatTyp
        DeedCatTypOutput = dts_diablo_instance.fn_fetch(query="""SELECT t.CntyCd,t.RecordingDt,t.DeedCatTyp,COUNT(*) AS [Count]
                    FROM tTrans.Trans t WITH (NOLOCK)
                    WHERE t.BatchDt >= CONVERT(INT, REPLACE(CONVERT(CHAR(10),DATEADD(WEEK, -6, GETDATE()), 121), '-', ''))
                    GROUP BY t.CntyCd,t.RecordingDt,t.DeedCatTyp""", fetch="fetchall", commit=False, close_conn=True)

        #Write results to text file
        file = f'{self.localpath}DeedCatTyp.{self.todayyyymmdd}.txt'
        self.file_util.WriteCSVfile(file, DeedCatTypOutput)

        #Copy file to SFTP
        log.info(f"Copying {file} to SFTP...")
        remotepath = os.path.join(self.ftp_path, Path(file).name)
        sftp_conn = ftp(host=self.ftp_host, user=self.ftp_user, pw=self.ftp_pswd, port=self.ftp_port, remotefolder=self.ftp_path, log=log)
        sftp_conn.putfile(localpath = file, remotepath = remotepath, close_conn = True)  

        finish = time.perf_counter()
        log.info(f'Func={func} extract completed, finished in {round(finish-start, 2)} second(s)')


    def PrimaryCatCd(self):

        func = inspect.getframeinfo(inspect.currentframe()).function
        log.info(f"Func={func} extract starting...")
        start = time.perf_counter()

        dts_diablo_instance = Database.connect_diablo(app_name=self.app_name + ' PrimaryCatCd', log=log)
        #Query for PrimaryCatCdOutput
        PrimaryCatCdOutput = dts_diablo_instance.fn_fetch(query="""SELECT t.CntyCd,t.RecordingDt,t.PrimaryCatCd,COUNT(*) AS [Count]
                    FROM tTrans.Trans t WITH (NOLOCK)
                    WHERE t.BatchDt >= CONVERT(INT, REPLACE(CONVERT(CHAR(10),DATEADD(WEEK, -6, GETDATE()), 121), '-', ''))
                    GROUP BY t.CntyCd,t.RecordingDt,t.PrimaryCatCd""", fetch="fetchall", commit=False, close_conn=False)

        #Write results to text file
        file = f'{self.localpath}PrimaryCatCd.{self.todayyyymmdd}.txt'        
        self.file_util.WriteCSVfile(file, PrimaryCatCdOutput)

        #Copy file to SFTP
        log.info(f"Copying {file} to SFTP...")
        sftp_conn = ftp(host=self.ftp_host, user=self.ftp_user, pw=self.ftp_pswd, port=self.ftp_port, remotefolder=self.ftp_path, log=log)        
        remotepath = os.path.join(self.ftp_path, Path(file).name)
        sftp_conn.putfile(localpath = file, remotepath = remotepath, close_conn = False)  

        finish = time.perf_counter()
        log.info(f'Func={func} extract completed, finished in {round(finish-start, 2)} second(s)')


    def DocTyp(self):

        func = inspect.getframeinfo(inspect.currentframe()).function
        log.info(f"Func={func} extract starting...")
        start = time.perf_counter()

        dts_diablo_instance = Database.connect_diablo(app_name=self.app_name + ' DocTyp', log=log)
        #Query for DocTypOutput
        DocTypOutput = dts_diablo_instance.fn_fetch(query="""SELECT t.CntyCd,t.RecordingDt,t.DocTyp,COUNT(*) AS [Count]
                    FROM tTrans.Trans t WITH (NOLOCK)
                    WHERE t.BatchDt >= CONVERT(INT, REPLACE(CONVERT(CHAR(10),DATEADD(WEEK, -6, GETDATE()), 121), '-', ''))
                    GROUP BY t.CntyCd,t.RecordingDt,t.DocTyp""", fetch="fetchall", commit=False, close_conn=True)

        #Write results to text file
        file = f'{self.localpath}DocTyp.{self.todayyyymmdd}.txt'        
        self.file_util.WriteCSVfile(file, DocTypOutput)

        #Copy file to SFTP
        log.info(f"Copying {file} to SFTP...")
        sftp_conn = ftp(host=self.ftp_host, user=self.ftp_user, pw=self.ftp_pswd, port=self.ftp_port, remotefolder=self.ftp_path, log=log)        
        remotepath = os.path.join(self.ftp_path, Path(file).name)
        sftp_conn.putfile(localpath = file, remotepath = remotepath, close_conn = True)  

        finish = time.perf_counter()
        log.info(f'Func={func} extract completed, finished in {round(finish-start, 2)} second(s)')


    def main(self):

        start = time.perf_counter() 
        rc = 0

        try:
            # Create the 3 extracts in parallel
            with ThreadPoolExecutor(max_workers=3) as executor:
                p1 = executor.submit(self.DeedCatTyp)
                p2 = executor.submit(self.PrimaryCatCd)            
                p3 = executor.submit(self.DocTyp)  
            if p1.exception() is not None:
                raise p1.exception()
            if p2.exception() is not None:
                raise p2.exception()
            if p3.exception() is not None:
                raise p3.exception()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)


obj = DocumentCountsExport()
obj.main()
