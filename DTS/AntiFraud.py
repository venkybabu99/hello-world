# Initial Author: Leo Martinez
# Date Written: April 2021
# Overview: 1.  Rewrite AntiFraud SSIS Package to Python
#           2.  This process
#               a.  Get files from Ftp
#               b.  Load files in the import db
#               c.  Update Files table
#
# History: To be completed for each change made after initial release
# Who: When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=AntiFraud  I/O
# SQL DB=QCPro      I
# SQL DB=Eagle      I
#
# Changes to the original SSIS packages
# We have stopped loading the Riverside County files are is no longer process data for that county (06065)
# SellerName load is also no longer being loaded
# Only 3 files are being loaded at this time (srea, target_la and target_la_sb827)
"""
I’ve attached the last file I found on the ftp for srea, target_la, target_la_sb827. 
Comments from Michael Hall on 10/21/2021
– All files look good.  My only suggestion would be to load the 10/05/2021 target file I attached here so that we are running parallel with current production.  Recording date 10/15/2021 is still a week or so ahead of our current status.

For the sellername file that we talked about yesterday I see that the most recent file on the ftp is from 2015-09-01 (please see attached, DSI_LO090115.ZIP) 
Comments from Michael Hall on 10/21/2021
– While I’m not able to confirm the source of this file, I believe it’s safe to say we no longer use this file as the most recent was from 6 years ago.   It also seems to be insignificant as it only contains one string of data. 

The not attached the riversideweeklytaxroll file because is too big to attached. 
Comments from Michael Hall on 10/21/2021
– As stated during the call, Riverside is no longer relevant or of importance as we no longer process data for this county.
"""
##############################################################################
import concurrent.futures
import fnmatch
import os
import pathlib
import pandas as pd
import stat
import time
import sys
from glob import glob
from datetime import datetime
import numpy as np
from pathlib import Path
import os
import json
import ast
import inspect
from datetime import timedelta
#
from utils.AddressPlus import AddressPlus
from utils import ParseName
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

log = get_logger('AntiFraud')

class AntiFraud:

    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.today = datetime.now()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)        
        self.temp_path = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.temp_path, createdir=True)
        """ Get number of days to look back for ftp files based on the modification timestamp"""
        self.ftpfilesdaysback = (self.today - timedelta(days=int(self.app_config.get_parm_value(section=self.app_name,parm="ftpfilesdaysback")))).timestamp()


    def copy_octarget_doc_from_1900(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        qcpro_instance = Database.connect_dts_qcpro(app_name=f'{self.app_name}_CopyOC', log=log)
        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_CopyOC', log=log)

        log.info('Get Data (GetAntiFraudData)...')
        pstart = time.perf_counter() 

        query = "EXEC GetAntiFraudData"
        df = qcpro_instance.fn_populate_dataframe(query=query)
        rows_select = len(df)
        total_cols = len(df.columns)
        finish = time.perf_counter()
        log.info(f'End Read File, Elapsed: {round(finish-pstart, 2)} second(s), Rows {rows_select:,d} with {total_cols:,d} columns')
        qcpro_instance.fn_close()

        desttable = self.app_config.get_parm_value(section=self.app_name,parm="table_tmp_TempOC1900")

        if rows_select > 0:
            sp_CopyOCTargetDocuments = self.app_config.get_parm_value(section=self.app_name,parm="sp_CopyOCTargetDocuments")

            log.info(f'Inserting to...{desttable}')
            pstart = time.perf_counter() 

            dts_antrifraud_instance.fn_load_dataframe_to_table(dest_table_name=desttable, dataframe=df, close_conn=False, truncate_table=True, commit=True) 

            query = f'SELECT COUNT(1) FROM {desttable}'
            rows_written = dts_antrifraud_instance.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)

            finish = time.perf_counter()                
            log.info(f'End Inserting, Elapsed: {round(finish-pstart, 2)} second(s), Rows Written:{rows_written:,d}')

            log.info(f'EXEC {sp_CopyOCTargetDocuments}...')
            query = f'EXEC {sp_CopyOCTargetDocuments}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)
        else:
            query = f'truncate table {desttable}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        dts_antrifraud_instance.fn_close()
        
        finish = time.perf_counter()
        return f'Func={func}, finished in {round(finish-start, 2)} second(s)'


    def get_max_recordId_from_itrsubset(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_MaxRecrdId', log=log)
        eagle_instance = Database.connect_dts_eagle(app_name=f'{self.app_name}_MaxRecrdId', log=log)

        query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_GetItrSubsetMaxRecordId")}'
        maxRecordId = dts_antrifraud_instance.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)

        log.info('Get Data (GetItrSubset)...')
        pstart = time.perf_counter() 

        query = f"SET NOCOUNT ON; EXEC GetItrSubset {maxRecordId}"
        df = eagle_instance.fn_populate_dataframe(query=query)
        rows_select = len(df)
        total_cols = len(df.columns)
        finish = time.perf_counter()
        log.info(f'End Read File, Elapsed: {round(finish-pstart, 2)} second(s), Rows {rows_select:,d} with {total_cols:,d} columns')
        eagle_instance.fn_close()

        if rows_select > 0:
            """ Convert BatchDate from datetime to date """
            df['BatchDate'] = pd.to_datetime(df['BatchDate']).dt.date
            """ Convert NaN to NULL """
            df['MortgageDocumentNumber'] = df['MortgageDocumentNumber'].replace({np.nan:None})
            df['DocumentNumber'] = df['DocumentNumber'].replace({np.nan:None})
            
            desttable = self.app_config.get_parm_value(section=self.app_name,parm="table_itrsubset")

            log.info(f'Inserting to...{desttable}')
            pstart = time.perf_counter() 
            dts_antrifraud_instance.fn_load_dataframe_to_table(dest_table_name=desttable, dataframe=df, truncate_table = False, commit=True, close_conn=False)             
            finish = time.perf_counter()
            log.info(f'End Inserting, Elapsed: {round(finish-pstart, 2)} second(s)')
        else:
            log.info('No Records to insert...')
        
        dts_antrifraud_instance.fn_close()

        finish = time.perf_counter()
        return f'Func={func}, finished in {round(finish-start, 2)} second(s)'


    # def get_riverside_weekly_taxroll(self):
    
    #     start = time.perf_counter()  
    #     func = inspect.getframeinfo(inspect.currentframe()).function        
    #     log.info(f'Func={func} Started...')

    #     """ Get the current time and get Weekday """
    #     t = datetime.time(datetime.now())
    #     dow = self.today.weekday()

    #     """ Task Download & Import Weekly Riverside Tax  """        
    #     """ Exec function only on Tue-Sat and between 0100 and 0130
    #         (DATEPART( "Hh", getdate() ) == 1) && (DATEPART( "mi", getdate() ) <= 30) && (DATEPART( "dw", getdate() ) > 2)
    #     """
    #     # if not (dow >= 1 and dow <= 5 and t.hour == 1 and t.minute <= 30):
    #     #     return

    #     cfg = json.loads(self.app_config.get_parm_value(section=self.app_name,parm="pid90_cfg"))

    #     file_cd = cfg[0]
    #     file_pid = cfg[1]
    #     zip_password = cfg[2]

    #     """ ftp.py """
    #     ftp_port = 22
    #     ftp_host = cfg[3]
    #     ftp_user = cfg[4]

    #     ftp_cred = self.vault_util.get_secret(ftp_user)
    #     ftp_pswd = ftp_cred['password']         
    #     ftp_path = cfg[5]
    #     ftp_mask = cfg[6]
    #     ftp_autoupdate = cfg[7]
    #     ftp_mode = cfg[8]

    #     filemask = cfg[9]
    #     file_fips = cfg[10]
    #     file_countyid = cfg[11]

    #     widths = json.loads(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_width"))
    #     header = ast.literal_eval(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_header"))
    #     desttable = self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_table")
    #     desttable_list = desttable.split('.')
    #     dest_schema = desttable_list[0].strip('][')
    #     dest_table = desttable_list[1].strip('][')        
    #     local_path = self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_localpath")
    #     self.file_util.CheckDirExists(folder=local_path, createdir=True)
    #     email = json.loads(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_email"))
    #     sp_taxupdate06065 = self.app_config.get_parm_value(section=self.app_name,parm="pid90_sp_taxupdate06065")

    #     """ Get email notification """
    #     emailFrom = email[0]
    #     emailTo = email[1]
    #     emailCC = email[2]
    #     emailBcc = email[3]
    #     emailSubject = email[4]
    #     emailBody = email[5]

    #     """ Connect to SFtp """
    #     sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=log)

    #     self.FoundNewAssesmentFile = False
    #     """ Get Ftp files list """
    #     # for f in sftp_conn.listdir_attr(ftp_path):
    #     ftp_filelist = sftp_conn.listdir_attr(remotefolder=ftp_path, close_conn=True)            
    #     for f in ftp_filelist:        
    #         """ Bypass if is a directory or does not match file mask """
    #         if stat.S_ISDIR(f.st_mode) or not fnmatch.fnmatch(f.filename, ftp_mask):
    #             continue
    #         local_file_path = os.path.join(local_path, f.filename)
    #         if ( ( not os.path.isfile(local_file_path) ) or ( f.st_mtime > os.path.getmtime(local_file_path ) ) ):
    #             log.info(f'Downloading {f.filename}...')
    #             """Check if the sfpt is open by doing a listdir, if is not open the open the sftp"""
    #             active_sftp_conn = sftp_conn.is_sftp_conn_active()
    #             if not active_sftp_conn:
    #                 sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=log)
    #             sftp_conn.getfile(remotepath=f.filename, localpath=local_file_path,close_conn=False)
    #             self.FoundNewAssesmentFile = True
        
    #     sftp_conn.close()

    #     """ Bypass Load since we did not find a new Assestment file """
    #     if not self.FoundNewAssesmentFile:
    #         return

    #     dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_RiverSide', log=log)

    #     log.info(f'Getting File List...{filemask}')
    #     for file in glob(os.path.join(local_path, filemask)):
    #         path = pathlib.Path(file)
    #         if (os.path.getsize(file) <= 0):
    #             continue

    #         log.info(f'Read File...{file}')
    #         pstart = time.perf_counter() 

    #         df = pd.read_csv(file, header = 0, usecols = header, dtype = str, keep_default_na = False)
    #         num_rows = len(df)
    #         finish = time.perf_counter()
    #         log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), {num_rows:,d} rows by {len(df.columns):,d} columns, {round(sys.getsizeof(df) / 1024 ** 2, 1)} MB')

    #         """ Add columns to the DataFrame """
    #         df.insert(0, 'Fips', file_fips, True)
    #         df.insert(1, 'MailCity_State', df['MailCity'] + ' ' + df['MailState'], True)

    #         """ Rename columns to be able to build the insert statement """
    #         df.columns = ['Fips','MailCity_State','AssessmentNumber','ConveyanceNumber','MailAddress','MailCity','MailState','MailZipCode','SitusHouseNumber','StreetDirection','StreetName','StreetNameSuffix','UnitNumber','ZipCode','CityName','AssessmentDescription','FirstAssesseeName','SecondAssesseeName','ThirdAssesseeName','FourthAssesseeName']

    #         log.info('Inserting...')
    #         pstart = time.perf_counter() 
    #         # dts_antrifraud_instance.fn_load_dataframe_to_table(dest_table_name=desttable, dataframe=df, truncate_table=True, commit=True, close_conn=False)
    #         dts_antrifraud_instance.fn_to_sql(dest_schema=dest_schema, dest_table_name=dest_table, dataframe=df, chunk_limit=100000, truncate_table=True, commit=True, close_conn=False)

    #         query = f'SELECT COUNT(1) FROM {desttable}'
    #         rows_written = dts_antrifraud_instance.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)

    #         if num_rows != rows_written:
    #             error = f'Failed writting all rows...Read={num_rows:,d}, Written{rows_written:,d}'
    #             log.exception(error)
    #             raise ValueError(error)
    #             # exit('Process Failed...')

    #         finish = time.perf_counter()
    #         log.info(f'End Insert File, Elapsed: {round(finish-pstart, 2)} second(s), Rows Written:{rows_written:,d}')

    #         dts_antrifraud_instance.fn_execute(query=sp_taxupdate06065, commit=True, close_conn=False)

    #         if emailTo:
    #             self.email_util.sendemail(ToAddress=emailTo, Subject=emailSubject, CCAddress=emailCC, FromAddress=emailFrom, BCCAddress=emailBcc, Body=emailBody.format(file))
                
    #     dts_antrifraud_instance.fn_close()

    #     finish = time.perf_counter()
    #     return f'Func={func}, finished in {round(finish-start, 2)} second(s)'


    def load_srea_files(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_srea', log=log)
       
        """ Load SREA files """
        self.find_and_load_by_file_id(dbconn=dts_antrifraud_instance, pid='7')

        query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_TaxUpdateNewRecordCount")}'
        self.sreanewrows = dts_antrifraud_instance.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)
        if self.sreanewrows > 0:            
            query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_TaxImportUpdate06037")}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        dts_antrifraud_instance.fn_close()
                
        finish = time.perf_counter()
        return f'Func={func}, finished in {round(finish-start, 2)} second(s)'


    def load_rest_of_the_files(self, pid: str):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_PID{pid}', log=log)
       
        self.find_and_load_by_file_id(dbconn=dts_antrifraud_instance, pid=pid)
                
        dts_antrifraud_instance.fn_close()

        finish = time.perf_counter()
        return f'Func={func}, finished in {round(finish-start, 2)} second(s)'
 


    def find_and_load_by_file_id(self, dbconn, pid: str):
        
        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')
        
        cfg = json.loads(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{pid}_cfg"))

        file_cd = cfg[0]
        file_pid = cfg[1]
        zip_password = cfg[2]

        log.info(f"* Loading Files, PID={file_pid}, CD='{file_cd}'... *")

        """ ftp.py """
        ftp_port = 22
        ftp_host = cfg[3]
        ftp_user = cfg[4]

        ftp_cred = self.vault_util.get_secret(ftp_user)
        ftp_pswd = ftp_cred['password']         
        ftp_path = cfg[5]
        ftp_mask = cfg[6]
        ftp_autoupdate = cfg[7]
        ftp_mode = cfg[8]

        filemask = cfg[9]
        file_fips = cfg[10]
        file_countyid = cfg[11]

        local_path = self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_localpath")
        self.file_util.CheckDirExists(folder=local_path, createdir=True)

        # query = f"EXEC GetFilesByProcess {file_pid}"
        # df = dbconn.fn_fetch(query=query, fetch='fetchall', commit=True, close_conn=False)

        """ Connect to SFtp """
        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=log)
        """ Get Ftp files list """
        ftp_filelist = sftp_conn.listdir_attr(remotefolder=ftp_path, close_conn=True)
        for f in ftp_filelist:
            if fnmatch.fnmatch(f.filename, ftp_mask) and f.st_mtime >= self.ftpfilesdaysback:
                # if not self.check_for_file_loaded_sql(f.filename, file_pid):      
                statusid = 2 if file_pid == 2 or file_pid == 9 or file_pid == 13 or file_pid == 90 else file_pid
                filetypeid = 2 if Path(f.filename).suffix.lower() == '.zip' else 1

                parm = 'check_file_processed_txt' if filetypeid == 1 else 'check_file_processed'
                query = self.app_config.get_parm_value(section=self.app_name,parm=parm).format(f.filename, file_pid, filetypeid, statusid)
                fileid = dbconn.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)        
                # log.info(f'Ftp File={f.filename}, PID={file_pid}, File {"" if fileid else "Not "} Loaded...')
                if not fileid:
                    fullname = os.path.join(local_path, f.filename)
                    """ Download the file """
                    log.info(f'Downloading file...{f.filename}')
                    pstart = time.perf_counter() 
                    """Check if the sfpt is open by doing a listdir, if is not open the open the sftp"""
                    active_sftp_conn = sftp_conn.is_sftp_conn_active()
                    if not active_sftp_conn:
                        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=log)
                    sftp_conn.getfile(remotepath=f'{ftp_path}/{f.filename}', localpath=fullname, close_conn=False)
                    finish = time.perf_counter()
                    log.info(f'Downloaded Completed, Elapsed: {round(finish-start, 2)} second(s)')
                    """ Process file """
                    self.process_ftp_file(dbconn=dbconn, file_pid=file_pid, fullinpfilename=fullname, zip_password=zip_password, local_path=local_path)
        """ Close Sftp Connection """
        sftp_conn.close()

        finish = time.perf_counter()
        return f'Func={func}, File {pid}, finished in {round(finish-start, 2)} second(s)'


    def process_ftp_file(self, dbconn, file_pid: int, fullinpfilename: str, zip_password: str, local_path: str):

        fileid = self.AddFile(dbconn=dbconn, file_pid=file_pid, filetypeid=1, filename=fullinpfilename, parentfileid=None)

        if fileid is None or fileid <= 0:
            error = f'Fileid is empty, Error inserting row, (self.AddFile)...'
            log.exception(error)
            raise ValueError(error)            

        if pathlib.Path(fullinpfilename).suffix.lower() != '.zip':
            self.process_file(dbconn=dbconn, file_pid=file_pid, fullfilename=fullinpfilename, fileid=fileid, srcezipped=False)
        else:
            listoffileNames = self.zip_utilZipNameList(fullinpfilename)
            for filename in listoffileNames:
                fullfilename = os.path.join(local_path, filename)
                log.info(f'Unzipping file...{pathlib.Path(fullinpfilename).name}, Txt={filename}')
                pstart = time.perf_counter() 
                self.zip_utilZipExtract(zipfile=fullinpfilename, filename=filename, destpath=local_path, password=zip_password )
                finish = time.perf_counter()
                log.info(f'Unzipping Completed, Elapsed: {round(finish-pstart, 2)} second(s)')
                """ Insert data to table """
                self.process_file(dbconn=dbconn, file_pid=file_pid, fullfilename=fullfilename, fileid=fileid, srcezipped=True)
            os.remove(fullfilename)

        os.remove(fullinpfilename)
        return


    def process_file(self, dbconn, file_pid:int, fullfilename: str, fileid: int, srcezipped: bool):

        """ If source ftp file is zip then we need to create a record for the txt file """
        if srcezipped:
            fileid2 = self.AddFile(dbconn=dbconn, file_pid=file_pid, filetypeid=2, filename=fullfilename, parentfileid=fileid)
        else:
            fileid2 = fileid

        if (os.path.getsize(fullfilename) > 0):
            self.load_file(dbconn=dbconn, file_pid=file_pid, fullfilename=fullfilename, fileid=fileid, fileid2=fileid2)

        self.SetFileStatus(dbconn=dbconn, fileid=fileid2, statusid=2 if file_pid == 2 or file_pid == 9 or file_pid == 13 else file_pid)

        return
        

    def load_file(self, dbconn, file_pid:int, fullfilename: str, fileid: int, fileid2: int):

        widths = json.loads(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_width"))
        header = ast.literal_eval(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_header"))
        desttable = self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_table")
        postprocessing = self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_postprocessing")

        cfg = json.loads(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_cfg"))
        file_fips = cfg[10]
        file_countyid = cfg[11]

        log.info(f'FileId={fileid}, FileId2={fileid2}, File={fullfilename}')
        pstart = time.perf_counter() 

        """ For file_pid = 7 got the following error
        https://stackoverflow.com/questions/61264795/pandas-unicodedecodeerror-utf-8-codec-cant-decode-bytes-in-position-0-1-in
        Resolution to add the encoding paramater
        https://github.com/modin-project/modin/issues/976
        """
        """ Turn out to be a columns had garbage (SitusKey) """
        # df = pd.read_fwf(fullfilename, widths=widths, header=None, names=header, dtype=str, keep_default_na = False, delimiter="\n\t", encoding = 'unicode_escape' )

        df = pd.read_fwf(fullfilename, widths=widths, header=None, names=header, dtype=str, keep_default_na = False, delimiter="\n\t")
        rows_read = len(df)
        total_cols = len(df.columns)
        finish = time.perf_counter()
        log.info(f'End Read File, Elapsed: {round(finish-pstart, 2)} second(s), Rows {rows_read:,d} with {total_cols:,d} columns')            

        """ Add column Fileid to the DataFrame """
        df.insert(0, 'fileid', fileid2, True)
        
        path = pathlib.Path(fullfilename)
        if file_pid == 7:    #file_cd == 'srea':
            """ Filter out empty RecordingDate (LEN(RTRIM(RecordingDate)) == 0) """
            df = df[df.RecordingDate != '        ']
            """ Since we drop rows based on the Recording Date, we are getting a new rows_read """
            rows_read = len(df)
            df.insert(1, 'Fips', file_fips, True)
            """ Remove Document Type """
            df.pop('DocumentType')
        elif ( file_pid == 2 or file_pid == 13 or file_pid == 9 ): # file_cd == "target_la" or file_cd == "target_riverside" or file_cd == "target_la_sb827" ):
            """ SSIS package expression
                (DT_I4)(RIGHT(SUBSTRING(RIGHT(@[User::LAFileName],12),1,8),4) + SUBSTRING(RIGHT(@[User::LAFileName],12),1,4)) """
            """ Get the recording dt from the file name """
            df.insert(4, 'RecordingDate', path.stem[4:8] + path.stem[0:4], True )
            df.insert(5, 'Fips', file_fips, True)
            df.insert(6, 'CountyId', file_countyid, True)
            """ 2000 + [Document Year] """
            df['DocumentYear'] = (2000 + df['DocumentYear'].astype(int) )
        
        pstart = time.perf_counter() 
        log.info('Start inserting rows...')

        """ Load data from Dataframe """            
        dbconn.fn_load_dataframe_to_table(dest_table_name=desttable, dataframe=df, truncate_table=False, commit=True, close_conn=False)

        query = f'SELECT COUNT(1) FROM {desttable} WHERE FileId = {fileid2}'
        rows_written = dbconn.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)
        finish = time.perf_counter()
        log.info(f'End Insert, Elapsed: {round(finish-pstart, 2)} second(s), Rows Written:{rows_written:,d}')

        if rows_read != rows_written:
            error = f'FileId2={fileid2}, Failed writting all rows...Read={rows_read:,d}, Written{rows_written:,d}'
            log.exception(error)
            raise ValueError(error)

        email = json.loads(self.app_config.get_parm_value(section=self.app_name,parm=f"pid{file_pid}_email"))

        """ Get email notification """
        emailFrom = email[0]
        emailTo = email[1]
        emailCC = email[2]
        emailBcc = email[3]
        emailSubject = email[4]
        emailBody = email[5]

        if emailTo:
            self.email_util.sendemail(ToAddress=emailTo, Subject=emailSubject, CCAddress=emailCC, FromAddress=emailFrom, BCCAddress=emailBcc, Body=emailBody.format(fullfilename))

        if postprocessing:
            if file_pid != 5:   # file_cd != 'sellername'
                query = postprocessing.format(fileid2, file_countyid)
            else:
                query = postprocessing
            dbconn.fn_execute(query=query, commit=True, close_conn=False)

        return


    def ParseNames(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_ParseNames', log=log)

        """ Pre-populate ParsedNames table for name parsing """
        query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_PrePopulateParsedNames")}'
        dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        """ Get Names to parse """
        log.info('Getting Names to Parse...')
        pstart = time.perf_counter() 
        query = self.app_config.get_parm_value(section=self.app_name,parm="get_names_to_parse")
        df = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        num_rows = len(df)
        finish = time.perf_counter()
        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows {num_rows:,d} with {len(df.columns):,d} columns')

        if num_rows == 0:
            log.info('No names found...')
            dts_antrifraud_instance.fn_close()
            return

        """ Get Corp Tags """
        log.info('Getting Corp Tags...')
        pstart = time.perf_counter() 
        query = self.app_config.get_parm_value(section=self.app_name,parm="get_corp_tags")
        df_corplist = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        finish = time.perf_counter()
        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows {len(df_corplist):,d} with {len(df_corplist.columns):,d} columns')

        """ Insert Name columns to the DataFrame """
        df.insert(5, 'Name1', None, False)
        df.insert(6, 'Name2', None, False)
        df.insert(7, 'Name3', None, False)
        df.insert(8, 'Name4', None, False)

        startread = time.perf_counter()
        log.info('Start parsing Names...')

        for row in df.itertuples():

            rawnames = []
            if row.RawName1 is not None and row.RawName1 > '':
                rawnames.append(row.RawName1)
            if row.RawName2 is not None and row.RawName2 > '':
                rawnames.append(row.RawName2)
            if row.RawName3 is not None and row.RawName3 > '':
                rawnames.append(row.RawName3)
            if row.RawName4 is not None and row.RawName4 > '':
                rawnames.append(row.RawName4)

            names = []
            for rawname in rawnames:
                tnames = ParseName.ParseName(df_corplist, rawname)
                for tname in tnames:
                    names.append(tname)

            if len(names) >= 1:
                df.at[row.Index, 'Name1'] = names[0]
            if len(names) >= 2:
                df.at[row.Index, 'Name2'] = names[1]
            if len(names) >= 3:
                df.at[row.Index, 'Name3'] = names[2]
            if len(names) >= 4:
                df.at[row.Index, 'Name4'] = names[3]
            if row.Index % 50000 == 0:
                if row.Index > 0:
                    finish = time.perf_counter()
                    log.info(f'Row Index: {row.Index:,d}, Elapsed: {round(finish-pstart, 2)} second(s)')
                pstart = time.perf_counter() 

        finish = time.perf_counter()
        log.info(f'End Processing, Elapsed: {round(finish-startread, 2)} second(s)')

        tmpparsedname = '[dbo].[TEMP_NAME_PARSED]'
        query = f"IF OBJECT_ID('{tmpparsedname}','U') IS NOT NULL DROP TABLE {tmpparsedname}; CREATE TABLE {tmpparsedname} ([RecordId] int NOT NULL PRIMARY KEY, [RawName1] varchar(100) NULL, [RawName2] varchar(100) NULL, [RawName3] varchar(100) NULL, [RawName4] varchar(100) NULL, [Name1] varchar(100) NULL, [Name2] varchar(100) NULL, [Name3] varchar(100) NULL, [Name4] varchar(100) NULL)"

        """ Define temp table """
        dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)
        """ Load data """
        dts_antrifraud_instance.fn_load_dataframe_to_table(dest_table_name=tmpparsedname, dataframe=df, truncate_table=False, commit=True, close_conn=False)            
        
        pstart = time.perf_counter() 
        log.info("Start Update...")
        query = self.app_config.get_parm_value(section=self.app_name,parm="update_parsed_names")
        dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)
        finish = time.perf_counter()
        log.info(f'End Update, Elapsed: {round(finish-pstart, 2)} second(s)')

        # """ Drop Temp Table """
        # query = f"IF OBJECT_ID('{tmpparsedname}','U') IS NOT NULL DROP TABLE {tmpparsedname};"
        # dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        dts_antrifraud_instance.fn_close()

        finish = time.perf_counter()
        return f'Func={func}, finished in {round(finish-start, 2)} second(s)'


    def SetFileStatus(self, dbconn, fileid: int, statusid: int):
        
        query=f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_SetFileStatus")} {fileid}, {statusid}'
        dbconn.fn_execute(query=query, commit=True, close_conn=False)


    def AddFile(self, dbconn, file_pid:int, filetypeid: int, filename: str, parentfileid: int = None) -> int:

        query=f"SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm='sp_AddFile')} {file_pid}, {filetypeid}, '{filename}', {'NULL' if parentfileid is None else parentfileid}"
        fileid = dbconn.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)

        return fileid


    def AddrStandarized(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_AddrStd', log=log)

        """ Process Address Standardization for Tax records """
        query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdProcessTax")}'
        dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        startdef = time.perf_counter()
        log.info("Start Process...")

        """ Reset Batches that were flagged as Sent
            This is new to the process since we are not standarizing the addresses offline
        """
        log.info('Reset Pending AddrPlus Batches...')
        query = "SET NOCOUNT ON; UPDATE [dbo].[AddressPlusBatch] SET SentDate = NULL WHERE CompletedDate IS NULL AND SentDate IS NOT NULL AND Records IS NOT NULL ;"
        dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        """ Get Pending Batch Id """
        log.info('Getting Pending AddrPlus Batches...')
        pstart = time.perf_counter() 
        query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdGetInputBatch")}'
        df = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        num_rows = len(df)
        finish = time.perf_counter()
        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows {num_rows:,d} with {len(df.columns):,d} columns')

        if num_rows == 0:
            log.info("No rows to process...")
            return
        
        addrstdstaging_table = self.app_config.get_parm_value(section=self.app_name,parm="addrstdstaging_table")
        query = f"TRUNCATE TABLE {addrstdstaging_table}"
        dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        """ Create empty DataFrame from table """
        query = f"SELECT TOP (0) * FROM {addrstdstaging_table}"
        df_stg = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        """ Create a column list from the DataFrame """
        df_list = df_stg.columns.values.tolist()

        startbtchs = time.perf_counter()
        log.info("Processing Batches...")
        for row_df in df.itertuples():

            startbtch = time.perf_counter()
            batchId = row_df.BatchId
            log.info(f"Processing BatchId: {batchId}")

            pstart = time.perf_counter() 
            log.info("Getting all pending addresses...")
            query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdGetInput")} {batchId}'
            df_addr = dts_antrifraud_instance.fn_populate_dataframe(query=query)
            num_rows = len(df_addr)
            finish = time.perf_counter()
            log.info(f'Rows: {num_rows:,d}, Elapsed: {round(finish-pstart, 2)} second(s)')
            if num_rows == 0:
                log.info("No rows to process...")
                continue

            """ Get FileId """
            query = f"SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm='sp_GetFileInfo')} 8, '{row_df.FileName}.txt'"
            df_fi = dts_antrifraud_instance.fn_populate_dataframe(query=query)
            num_rows = len(df_fi)
            if num_rows == 0:
                error = f'Unable to get FileId, query={query}'
                log.exception(error)
                raise ValueError(error)

            fileid = str(df_fi['FileId'].values[0])

            startrows = time.perf_counter()
            log.info(f"Standarizing addresses...Batch Id={batchId}, File Id={fileid}")
            for addr in df_addr.itertuples():

                fips = '' if isinstance(addr.fips, type(None)) else addr.fips
                pcl = '' if isinstance(addr.pcl, type(None)) else addr.pcl
                pcl_seq = '' if isinstance(addr.pcl_seq, type(None)) else addr.pcl_seq
                raw_sit_addr = '' if isinstance(addr.raw_sit_addr,type(None)) else addr.raw_sit_addr
                raw_sit_city = '' if isinstance(addr.raw_sit_city,type(None)) else addr.raw_sit_city
                raw_sit_st = '' if isinstance(addr.raw_sit_st,type(None)) else addr.raw_sit_st
                raw_sit_zip = '' if isinstance(addr.raw_sit_zip,type(None)) else addr.raw_sit_zip
                raw_mail_addr = '' if isinstance(addr.raw_mail_addr,type(None)) else addr.raw_mail_addr
                raw_mail_city = '' if isinstance(addr.raw_mail_city,type(None)) else addr.raw_mail_city
                raw_mail_st = '' if isinstance(addr.raw_mail_st,type(None)) else addr.raw_mail_st
                raw_mail_zip = '' if isinstance(addr.raw_mail_zip,type(None)) else addr.raw_mail_zip

                """ Create Empty dictionary to append address standarization columns """
                addrow = dict.fromkeys(df_list, None)

                addrow['AddressId'] = pcl
                addrow['FileId'] = fileid
                addrow['fips'] = fips
                addrow['raw_sit_addr'] = raw_sit_addr
                addrow['raw_sit_city'] = raw_sit_city
                addrow['raw_sit_st'] = raw_sit_st
                addrow['raw_sit_zip'] = raw_sit_zip
                addrow['raw_mail_addr'] = raw_mail_addr
                addrow['raw_mail_city'] = raw_mail_city
                addrow['raw_mail_st'] = raw_mail_st
                addrow['raw_mail_zip'] = raw_mail_zip

                siteonln, siteraw = AddressPlus.addrplus(fulladdress=raw_sit_addr,city=raw_sit_city,state=raw_sit_st,zipcode=raw_sit_zip,country='',foreignlastline='',addresstype='S')

                if ( raw_sit_addr == raw_mail_addr and raw_sit_city == raw_mail_city and raw_sit_st == raw_mail_st and raw_sit_zip == raw_mail_zip ):
                    mailonln = siteonln.copy()
                    mailraw = siteraw.copy()
                else:
                    mailonln, mailraw = AddressPlus.addrplus(fulladdress=raw_mail_addr,city=raw_mail_city,state=raw_mail_st,zipcode=raw_mail_zip,country='',foreignlastline='',addresstype='M')
                
                addrow = self.PopulatAddressRow(addrow=addrow, siteraw=siteraw, mailraw=mailraw)

                """ Append standarized address to DataFrame """
                df_stg = df_stg.append(addrow, ignore_index=True)

                if addr.Index % 5000 == 0:
                    if addr.Index > 0:
                        finish = time.perf_counter()
                        log.info(f"Row Index: {addr.Index:,d}, Elapsed: {round(finish-pstart, 2)} second(s)")
                        if len(df_stg) > 0:
                            """ Insert Rows to Staging """
                            pstart = time.perf_counter() 
                            log.info("---Insert Rows...")
                            dts_antrifraud_instance.fn_load_dataframe_to_table(dest_table_name=addrstdstaging_table, dataframe=df_stg, truncate_table=False, commit=True, close_conn=False)            
                            finish = time.perf_counter()
                            log.info(f'---Elapsed: {round(finish-pstart, 2)} second(s)')
                            """ Reset Dataframe """
                            df_stg = df_stg[0:0]
                    pstart = time.perf_counter() 

            finish = time.perf_counter()
            log.info(f'Completed standarizing addresses, FileId={fileid}, Elapsed: {round(finish-startrows, 2)} second(s)')

            """ Insert Dataframe to Staging """
            if len(df_stg) > 0:
                """ Insert Rows to Staging """
                pstart = time.perf_counter() 
                log.info("---Insert Rows...")
                dts_antrifraud_instance.fn_load_dataframe_to_table(dest_table_name=addrstdstaging_table, dataframe=df_stg, truncate_table=False, commit=True, close_conn=False)            
                finish = time.perf_counter()
                log.info(f'---Elapsed: {round(finish-pstart, 2)} second(s)')
                """ Reset Dataframe """
                df_stg = df_stg[0:0]

            """ Update inserted rows AddressStd """
            pstart = time.perf_counter() 
            log.info("Updating AddressStd Table...")
            query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdMatchOutput")}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)
            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s)')

            """ Flag FileId as completed """
            self.SetFileStatus(dbconn=dts_antrifraud_instance, fileid=fileid, statusid=8)

            finish = time.perf_counter()
            log.info(f'Completed batch process, Batch Id={batchId},  Elapsed: {round(finish-startbtch, 2)} second(s)')

        finish = time.perf_counter()
        log.info(f'Completed processing batches... Elapsed: {round(finish-startbtchs, 2)} second(s)')

        finish = time.perf_counter()
        log.info(f'Elapsed: {round(finish-startdef, 2)} second(s)')

        finish = time.perf_counter()
        return f'Func={func}, finished in {round(finish-start, 2)} second(s)'


    def PopulatAddressRow(self, addrow: dict, siteraw: dict, mailraw: dict) -> dict:

        addrow['sit_nbr'] = siteraw['PsdNumber']
        addrow['sit_nbr_to'] = siteraw['PsdNumberTo']
        addrow['sit_fract'] = siteraw['PsdNumberFraction']
        addrow['sit_predir'] = siteraw['Predir']
        addrow['sit_str'] = siteraw['Street']
        addrow['sit_sfx'] = siteraw['Suffix']
        addrow['sit_postdir'] = siteraw['Postdir']
        addrow['sit_unit'] = siteraw['Unitnumber']
        addrow['sit_addr'] = siteraw['FullAddress']
        addrow['sit_city'] = siteraw['City']
        addrow['sit_zip5'] = siteraw['ZIPCode']
        addrow['sit_zip4'] = siteraw['ZIP4']
        addrow['sit_cr_rt'] = siteraw['CRRT']
        addrow['sit_st'] = siteraw['State']
        addrow['sit_match'] = siteraw['MatchCode']
        addrow['sit_pfx'] = siteraw['PsdNumberPrefix']
        addrow['sit_error_code'] = siteraw['ErrorCode']
        addrow['sit_dpbc'] = siteraw['DPBC']
        addrow['sit_ln_trav'] = siteraw['LOT']
        addrow['sit_ln_trav_ind'] = siteraw['LOTOrder']
        addrow['cens_tr'] = siteraw['CensusTract']
        addrow['cens_blk'] = siteraw['CensusBlockGroup']
        addrow['cens_blk2'] = siteraw['CensusBlock2']
        addrow['cens_blk_sfx'] = siteraw['CensusBlockSuffix']
        
        if siteraw['Latitude']:
            latitude_split = siteraw['Latitude'].split('.')
            addrow['latitude'] = f'{latitude_split[0]}.{latitude_split[1][:6]}'
        else:
            addrow['latitude'] = siteraw['Latitude']

        addrow['longitude'] = siteraw['Longitude']
        addrow['geo_match_cd'] = siteraw['LocCode']
        addrow['cbsa'] = siteraw['CBSA']
        addrow['rdi'] = siteraw['RBDI']
        addrow['mail_nbr'] = mailraw['PsdNumber']
        addrow['mail_to'] = mailraw['PsdNumberTo']
        addrow['mail_fract'] = mailraw['PsdNumberFraction']
        addrow['mail_predir'] = mailraw['Predir']
        addrow['mail_str'] = mailraw['Street']
        addrow['mail_sfx'] = mailraw['Suffix']
        addrow['mail_postdir'] = mailraw['Postdir']
        addrow['mail_unit'] = mailraw['Unitnumber']
        addrow['mail_addr'] = mailraw['FullAddress']
        addrow['mail_city'] = mailraw['City']
        addrow['mail_st'] = mailraw['State']
        addrow['mail_zip5'] = mailraw['ZIPCode']
        addrow['mail_zip4'] = mailraw['ZIP4']
        addrow['mail_cr_rt'] = mailraw['CRRT']
        addrow['mail_match'] = mailraw['MatchCode']
        addrow['mail_pfx'] = mailraw['PsdNumberPrefix']
        addrow['mail_cntry'] = '' if mailraw['IsForeign'] == 'NO' else mailraw['Country']
        addrow['mail_csz'] = mailraw['ForeignCSZ']
        addrow['mail_last_line'] = mailraw['ForeignLastLine']
        addrow['mail_error_code'] = mailraw['ErrorCode']
        addrow['mail_dpbc'] = mailraw['DPBC']
        addrow['mail_ln_trav'] = mailraw['LOT']
        addrow['mail_ln_trav_ind'] = mailraw['LOTOrder']
        addrow['mail_cens_tr'] = mailraw['CensusTract']
        addrow['mail_cens_blk'] = mailraw['CensusBlockGroup']
        addrow['mail_cens_blk2'] = mailraw['CensusBlock2']
        addrow['mail_cens_blk_sfx'] = mailraw['CensusBlockSuffix']

        if mailraw['Latitude']:        
            latitude_split = mailraw['Latitude'].split('.')
            addrow['mail_latitude'] = f'{latitude_split[0]}.{latitude_split[1][:6]}'
        else:
            addrow['mail_latitude'] = mailraw['Latitude']

        addrow['mail_longitude'] = mailraw['Longitude']
        addrow['mail_geo_match_cd'] = mailraw['LocCode']
        addrow['msa'] = siteraw['MSA']
        addrow['std_sit_hse1'] = siteraw['Number']
        addrow['sit_dpv_confirm'] = siteraw['DPVConfirm']
        addrow['sit_dpv_cmra'] = siteraw['DPVCMRA']
        addrow['sit_dpv_footnote1'] = siteraw['DPVFootnote1']
        addrow['sit_dpv_footnote2'] = siteraw['DPVFootnote2']
        addrow['sit_lacslink_ind'] = siteraw['LACSLinkInd']
        addrow['mail_dpv_confirm'] = mailraw['DPVConfirm']
        addrow['mail_dpv_cmra'] = mailraw['DPVCMRA']
        addrow['mail_dpv_footnote1'] = mailraw['DPVFootnote1']
        addrow['mail_dpv_footnote2'] = mailraw['DPVFootnote2']
        addrow['mail_lacslink_ind'] = mailraw['LACSLinkInd']
        
        """ Online Address Std does not have these columns """
        addrow['sit_vanity_city'] = ''
        addrow['sit_cass_flag'] = ''
        addrow['sit_unit_type'] = ''
        addrow['sit_high_unit'] = ''
        addrow['sit_low_unit'] = ''
        addrow['adv_unit_nbr_fwd'] = ''
        addrow['mail_vanity_city'] = ''
        addrow['mail_cass_flag'] = ''
        addrow['mail_unit_type'] = ''
        addrow['mail_high_unit'] = ''
        addrow['mail_low_unit'] = ''
        addrow['mail_pr_urb_cd'] = ''        
        addrow['map_sec'] = ''        
        addrow['map_twp'] = ''        
        addrow['map_rng'] = ''        
        addrow['map_sec_qtr'] = ''        
        addrow['map_sec_16'] = ''        
        addrow['tx_area'] = ''        
        addrow['map_link'] = ''        
        addrow['hm_exmpt'] = ''        
        addrow['abs_occ'] = ''        
        addrow['map_ref1'] = ''        
        addrow['map_pg1'] = ''        
        addrow['map_grid1'] = ''        
        addrow['ownr_buyr_addr_dpv_cd'] = ''        

        """ Table does not have these columns """
        # addrow['sit_dpv_vacant'] = siteraw['DPVVacant']
        # addrow['mail_dpv_vacant'] = mailraw['DPVVacant']
        
        return addrow


    def GenerateExtract(self):

        startdef = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_Extr', log=log)

        """ Get Pending BatchId """
        pstart = time.perf_counter() 
        log.info("Getting Pending Extracts...")
        query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_GetReleasedExtracts")}'
        df = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        num_rows = len(df)
        finish = time.perf_counter()
        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows {num_rows:,d} with {len(df.columns):,d} columns')

        if num_rows == 0:
            log.info("No pending extracts found...")
            return

        extract_cfg = ast.literal_eval(self.app_config.get_parm_value(section=self.app_name,parm="extract_cfg"))
        ftp_host = extract_cfg[0]
        ftp_user = extract_cfg[1]
        ftp_cred = self.vault_util.get_secret(ftp_user)
        ftp_pswd = ftp_cred['password']         
        ftp_path = extract_cfg[2]
        ftp_port = 22

        startextrcs = time.perf_counter()
        log.info("Processing Extracts...")
        for row_df in df.itertuples():
            
            startbtch = time.perf_counter()
            countid = str(row_df.CountyId)
            recordingdate = str(row_df.RecordingDate)
            countyshortcd = str(row_df.ShortCd)
            ftp_path_full = ftp_path.format(countyshortcd)

            log.info(f"Processing Extract: CountyId: {countid}, RecordingDate: {recordingdate}, ShortCd: {countyshortcd}")

            """ Get FileId """
            pstart = time.perf_counter() 
            log.info("Get FileId...")
            query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddExtractFile")} {countid}, {recordingdate}'
            # df_ei = dts_antrifraud_instance.fn_populate_dataframe(query=query)
            df_ei = dts_antrifraud_instance.fn_fetch(query=query, fetch='fetchall', commit=True, close_conn=False, return_dataframe=True)            
            num_rows = len(df_ei)
            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows {num_rows:,d} with {len(df_ei.columns):,d} columns')

            if num_rows == 0:
                error = f'Unable to get ExtractId, query={query}'
                log.exception(error)
                raise ValueError(error)

            extractid = df_ei['ExtractId'].values[0]
            extractfilename = df_ei['FileName'].values[0]

            pstart = time.perf_counter() 
            log.info(f"Get Final Output, ExtractId: {extractid}")
            query = self.app_config.get_parm_value(section=self.app_name,parm="get_final_output").format(self.app_config.get_parm_value(section=self.app_name,parm="sp_GetFinalOutput"), countid, recordingdate)
            df_extr = dts_antrifraud_instance.fn_populate_dataframe(query=query)
            num_rows = len(df_extr)
            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows {num_rows:,d} with {len(df_extr.columns):,d} columns')

            if num_rows == 0:
                log.info("No rows to process...")
                continue

            """ Add extractid column to the DataFrame """
            df_extr.insert(0, 'ExtractId', extractid, True)

            pstart = time.perf_counter() 
            log.info("Importing Rows...")
            extractdata_table = self.app_config.get_parm_value(section=self.app_name,parm="extractdata_table")
            dts_antrifraud_instance.fn_load_dataframe_to_table(dest_table_name=extractdata_table, dataframe=df_extr, truncate_table=False, commit=True, close_conn=False)  
            finish = time.perf_counter()
            log.info(f'Completed... Elapsed: {round(finish-pstart, 2)} second(s)')

            pstart = time.perf_counter() 
            log.info(f"Get Extract by ExtractId, ExtractId: {extractid}")
            query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_GetExtractById")} {extractid}'
            df_extr = dts_antrifraud_instance.fn_populate_dataframe(query=query)
            num_rows = len(df_extr)
            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows {num_rows:,d} with {len(df_extr.columns):,d}  columns')
            
            if num_rows == 0:
                log.info("No rows to process...")
                continue

            pstart = time.perf_counter() 
            filename = os.path.join(self.temp_path, extractfilename + '.txt')
            log.info(f"Creating Extract file... {filename}")

            df_extr.replace(to_replace=[None], value='', inplace=True)

            with open(filename, 'w') as extr:
                for index, row in df_extr.iterrows():
                    line = f"{row['DocumentYear'][0:5]:<5}"
                    line += f"{row['DocumentNumber'][0:11]:<11}"
                    line += f"{row['DocumentSequence'][0:2]:<2}"
                    line += f"{row['Apn'][0:10]:<10}"
                    line += f"{row['Name1'][0:30]:<30}"
                    line += f"{row['Name2'][0:30]:<30}"
                    line += f"{row['Name3'][0:30]:<30}"
                    line += f"{row['Name4'][0:30]:<30}"
                    line += f"{row['Street'][0:40]:<40}"
                    line += f"{row['CityState'][0:24]:<24}"
                    line += f"{row['Zip5'][0:5]:<5}"
                    line += f"{row['Zip4'][0:4]:<4}"
                    line += f"{row['PageCount'][0:3]:<3}"
                    line += f"{row['DocumentType'][0:2]:<2}"
                    line += ' '
                    line += '\n'
                    extr.write(line)
                extr.close()

            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s)')

            tmpremotefilepath = os.path.join(ftp_path_full, extractfilename + '.tmp')
            remotefilepath = os.path.join(ftp_path_full, extractfilename + '.txt')

            pstart = time.perf_counter() 
            log.info(f"Ftp file... {filename}")

            """ Connect to SFtp """
            sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path_full, log=log)

            sftp_conn.putfile(localpath=filename, remotepath=tmpremotefilepath, close_conn=False)
            sftp_conn.rename(remotesrc=tmpremotefilepath, remotetrg=remotefilepath, close_conn=True)

            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s)')

            pstart = time.perf_counter() 
            log.info(f"Set Extract to Complete, ExtractId: {extractid}")

            query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_SetExtractCompleted")} {extractid}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s)')
            os.remove(filename)

        finish = time.perf_counter()
        log.info(f'Elapsed: {round(finish-startextrcs, 2)} second(s)')

        finish = time.perf_counter()
        log.info(f'Func={func}, Elapsed: {round(finish-startdef, 2)} second(s)')

        return


    def GatherLatestData(self):

        """ Indicators to know if we loaded a new file """
        self.FoundNewAssesmentFile = False
        self.sreanewrows = 0
        filestoload = json.loads(self.app_config.get_parm_value(section=self.app_name,parm="filesToLoad"))
        future_list = []        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            """ Task Copy OC Target Documents from 1900 """
            future = executor.submit(self.copy_octarget_doc_from_1900)
            future_list.append(future)
            """ Task Get Max RecordId from Itr Subset """
            future = executor.submit(self.get_max_recordId_from_itrsubset)            
            future_list.append(future)
            # Stop loading this file as we no longer process data for Riverside (06065)
            # """ Task Download & Import Weekly Riverside Tax  """        
            # future = executor.submit(self.get_riverside_weekly_taxroll)            
            # future_list.append(future)
            """ Load SREA files """
            future = executor.submit(self.load_srea_files)            
            future_list.append(future)
            """ Load the rest of the files"""
            for arg in filestoload:
                future = executor.submit(self.load_rest_of_the_files, arg[1])
                future_list.append(future)

        for future in future_list:
            try:
                log.info(future.result())
            except Exception as e:
                log.exception(e)
                raise

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_Rest', log=log)
        if self.sreanewrows > 0 or self.FoundNewAssesmentFile:
            """ Update Active flag on Tax table """
            query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_TaxUpdateActive")}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

            """ Archive Tax """
            query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_ArchiveTax")}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)
                    
        """ Match Target Records """
        query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_MatchTargetRecords")}'
        dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        dts_antrifraud_instance.fn_close()

        future_list = []        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            """ Name Parsing Utility """
            future = executor.submit(self.ParseNames)
            future_list.append(future)
            """ Address Standarization Input """
            future = executor.submit(self.AddrStandarized)            
            future_list.append(future)

        for future in future_list:
            try:
                log.info(future.result())
            except Exception as e:
                log.exception(e)
                raise

        return


    def main(self):

        start = time.perf_counter() 
        rc = 0        

        try:
            """ SC Gather Latest Data """
            self.GatherLatestData()

            """ Generate Extract """
            self.GenerateExtract()

            """ Archive Extracted Target Records """
            dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_Rest2', log=log)
            pstart = time.perf_counter() 
            log.info("Archive Extracted Target Records...")
            query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_ArchiveExtractedTargetRecords")}'
            dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=True)
            finish = time.perf_counter()
            log.info(f'Elapsed: {round(finish-pstart, 2)} second(s)')

        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)            


obj = AntiFraud()
obj.main()
