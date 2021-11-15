# Initial Author: Byron Centeno
# Date Written: May 2021
# Overview: 1. Rewrite dtsMCExtrat SSIS Package to Python
#           2. This process
#               a. Executes 2 queries in diablo
#               b. Inserts the results to Dataquick
#               c. Creates 2 seperate flat files with the output for each
#                  query
#               d. Uploads to FTP and deletes the files
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=dataquick  I/O
# SQL DB=readme     I/O
# SQL Server=live-diablo-dev.infosolco.com
# SQL DB=Diablo     I
##############################################################################
import datetime
import os
# ###################################
import time
from pathlib import Path
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

log = get_logger('DQMCExtract')

class DQMCExtract:

    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)        
        self.temp_path = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.temp_path, createdir=True)


    def load_diablo_dqcnty(self):
        
        log.info('Reading Diablo DqCnty Data...')
        pstart = time.perf_counter() 

        diablo_instance = Database.connect_diablo(app_name=self.app_name, log=log)
        query = 'SELECT CntyCd, AssignmentEffDt, DeedMtgEffDt, convert(varchar(8),DefaultEffDt) DefaultEffDt, ExcludeEfxTransDTran, ExcludeEfxTransDqAsn, ExcludeEfxTransDqDef, ExcludeEfxTransDqNot, IncludePrevTax, IsDocCnty, IsSAOnlyCnty, EFXMarketracCloseInd, convert(varchar(8),NoticeEffDt) NoticeEffDt, TaxDelinqInd, UpdateId, UpdateTimeStamp, Comment, Tier FROM [tCommon].[DQCnty] ;'
        df = diablo_instance.fn_populate_dataframe(query=query)
        diablo_instance.fn_close()
        
        rows_select = len(df)
        total_cols = len(df.columns)
        finish = time.perf_counter() 
        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows Read:{rows_select}, # of Columns:{total_cols}')

        dest_table_name = '[dbo].[DQCnty]'
        log.info(f'Writing to...{dest_table_name}')
        pstart = time.perf_counter() 
        
        dts_dataquick_instance = Database.connect_dts_dataquick(app_name=self.app_name, log=log)
        dts_dataquick_instance.fn_load_dataframe_to_table(dest_table_name=dest_table_name, dataframe=df, truncate_table = True, commit = True, close_conn = False) 

        query = f'SELECT COUNT(1) FROM {dest_table_name}'
        rows_written = dts_dataquick_instance.fn_fetch(query=query, fetch='fetchval', commit=False, close_conn=True)
            
        finish = time.perf_counter() 
        log.info(f'End Insert File, Elapsed: {round(finish-pstart, 2)} second(s), Rows Written:{rows_written}')

        if rows_select != rows_written:
            error = f'Rows do not match, process failed...Read={rows_select}, Written{rows_written}'
            log.exception(error)
            raise ValueError(error)
            # exit('Process Failed...')
        
        return

    def GenerateMCExtractFile(self):

        zip_pswd = self.app_config.get_parm_value(section=self.app_name,parm="zip_pswd").encode()
        localpath = self.app_config.get_parm_value(section=self.app_name,parm="localpath")
        folderfnd = self.file_util.CheckDirExists(folder=localpath, createdir=True)
        if not folderfnd:
            error = f"Folder not found '{localpath}', processed failed..."
            log.exception(error)
            raise ValueError(error)
            # exit('Process Failed...')

        query = 'SET NOCOUNT ON ; EXEC DQMCExtract WITH RESULT SETS(( [State] CHAR(2) NULL, [CountyFips] CHAR(3) NULL, [CloseBit] INT NULL, [CloseDate] CHAR(8) NULL, [RunDate] CHAR(8) NULL )) ;'

        dts_readme_instance = Database.connect_dts_readme(app_name=self.app_name, log=log)       
        df_extr = dts_readme_instance.fn_populate_dataframe(query=query)
        num_rows = len(df_extr)
        log.info(f'Rows Read:{num_rows}, # of Columns:{len(df_extr.columns)}')
        dts_readme_instance.fn_close()

        DQMCFile = f'MC_{str(datetime.datetime.now())[0:10].replace("-", "")}_2.txt'
        FullDQMCFile = os.path.join(localpath, DQMCFile)
        DQMCStats = Path(FullDQMCFile).stem + "STATS.txt"
        FullDQMCStats = os.path.join(localpath, DQMCStats)
        DQMCZipfile = Path(DQMCFile).stem + '.zip'

        ''' Create detail file '''
        with open(FullDQMCFile, 'w') as extr:
            for index, row in df_extr.iterrows():    
                line = f"{row['State'][0:2]:<2}"
                line += f"{row['CountyFips'][0:3]:<3}"
                line += f"{row['CloseBit']:<1}"      
                line += f"{row['CloseDate'][0:8]:<8}"      
                line += f"{row['RunDate'][0:8]:<8}"      
                line += '\n'
                extr.write(line)
            extr.close()              
        
        ''' Create stats file '''
        with open(FullDQMCStats, 'w') as extr:
            line = f'0,{num_rows},5'
            line += '\n'
            extr.write(line)
            extr.close()              

        ''' Zip file '''
        zipfile = os.path.join(localpath, DQMCZipfile)
        self.zip_util.Compress(srcefile=FullDQMCFile, srcefilepath=None, destfile=zipfile, password=zip_pswd, compression_level=5, deletesrce=False)

        # Setup sftp credentials
        ftp_port = 22
        ftp_host = self.app_config.get_parm_value(section=self.app_name,parm="ftp_host")        
        ftp_user = self.app_config.get_parm_value(section=self.app_name,parm="ftp_user")        
        cred = self.vault_util.get_secret(ftp_user)
        ftp_pswd = cred['password'] 
        ftp_path = self.app_config.get_parm_value(section=self.app_name,parm="ftp_path")

        ''' Connect to SFtp '''
        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=log)    

        remotepath = os.path.join(ftp_path, Path(zipfile).name)
        sftp_conn.putfile(localpath = zipfile, remotepath = remotepath, close_conn = False)  
        remotepath = os.path.join(ftp_path, Path(FullDQMCStats).name)
        sftp_conn.putfile(localpath = FullDQMCStats, remotepath = remotepath, close_conn = True)  

        return


    def main(self):

        start = time.perf_counter() 
        rc = 0        

        try:
            self.load_diablo_dqcnty()
            self.GenerateMCExtractFile()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)  


obj = DQMCExtract()
obj.main()
