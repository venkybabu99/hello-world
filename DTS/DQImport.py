# Initial Author: Leo Martinez
# Date Written: April 2021
# Overview: 1.  Rewrite DQTImport SSIS Package to Python
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
# SQL DB=import     I/O
##############################################################################
import fnmatch
import pathlib
import pandas as pd
import time
import concurrent.futures
import json
import ast
import os
import inspect
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

log = get_logger('DQImport')

class DQImport:

    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.table_name_files = self.app_config.get_parm_value(section=self.app_name,parm="table_name_files")
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)
        self.temp_path = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.temp_path, createdir=True)


    def process_ftp_file(self, filecd: str, fullzipfilename: str, pid: int):

        dts_import_instance = Database.connect_dts_import(app_name=f'{self.app_name}_pid{filecd}', log=log)
        query = self.app_config.get_parm_value(section=self.app_name,parm="insert_new_file").format(self.table_name_files, pid, 1, fullzipfilename, 'NULL')
        fileid = dts_import_instance.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=True)

        if fileid is None or fileid <= 0:
            error = f'Fileid is empty, Error inserting row, query={query}...'
            log.exception(error)
            raise ValueError(error)
            # exit('Fileid is empty...')

        if pathlib.Path(fullzipfilename).suffix.lower() != '.zip':
            return

        zip_password = self.app_config.get_parm_value(section=self.app_name,parm="zip_pswd").encode()
        listoffileNames = self.zip_util.ZipNameList(fullzipfilename)
        for filename in listoffileNames:
            fullfilename = os.path.join(self.temp_path, filename)
            log.info(f'Unzipping file...{pathlib.Path(fullzipfilename).name}, Txt={filename}')
            pstart = time.perf_counter()
            self.zip_util.ZipExtract(zipfile=fullzipfilename, filename=filename, destpath=self.temp_path, password=zip_password)
            elapsed = self.file_util.seconds_to_hhmmss(round(time.perf_counter()-pstart, 2))
            log.info(f'Unzipping Completed, Elapsed: {elapsed}')
            ''' Insert data to table '''
            self.load_file(pid, fullfilename, filecd, fileid)
            os.remove(fullfilename)

        os.remove(fullzipfilename)
        return


    def load_file(self, pid: int, fullfilename: str, filecd: str, fileid: int):

        dts_import_instance = Database.connect_dts_import(app_name=f'{self.app_name}_pid{filecd}', log=log)
        query = self.app_config.get_parm_value(section=self.app_name,parm="insert_new_file").format(self.table_name_files, pid, 2, fullfilename, fileid)
        fileid2 = dts_import_instance.fn_fetch(query=query, fetch='fetchval', commit=True, close_conn=False)

        widths = json.loads(self.app_config.get_parm_value(section=self.app_name,parm=f'file_{filecd}_width'))        
        header = ast.literal_eval(self.app_config.get_parm_value(section=self.app_name,parm=f'file_{filecd}_header'))

        if (os.path.getsize(fullfilename) > 0):
            log.info(f'FileId={fileid}, FileId2={fileid2}, File={fullfilename}')
            pstart = time.perf_counter()

            df = pd.read_fwf(fullfilename, widths=widths, header=None, names=header, dtype=str, keep_default_na = False, delimiter="\n\t" )

            rows_read = len(df)
            total_cols = len(df.columns)
            elapsed = self.file_util.seconds_to_hhmmss(round(time.perf_counter()-pstart, 2))
            log.info(f'End Read File, Elapsed: {elapsed}, Rows Read:{rows_read:,d}, # of Columns:{total_cols:,d}')

            ''' Add column Fileid to the DataFrame '''
            df.insert(0, 'fileid', fileid2, True)
            ''' Drop last column from the DataFrame '''
            df.pop('X')

            pstart = time.perf_counter()
            log.info('Start inserting rows...')

            ''' Build the insert from the DataFrame '''            
            desttable = self.app_config.get_parm_value(section=self.app_name,parm=f'table_name_file_{filecd}')
            desttable_list = desttable.split('.')
            dest_schema = desttable_list[0].strip('][')
            dest_table = desttable_list[1].strip('][')               

            # dts_import_instance.fn_load_dataframe_to_table(dest_table_name=desttable, dataframe=df, truncate_table=False, commit=True, close_conn=False)
            dts_import_instance.fn_to_sql(dest_schema=dest_schema, dest_table_name=dest_table, dataframe=df, chunk_limit=100000, truncate_table=False, commit=True, close_conn=False)

            query = f'SELECT COUNT(1) FROM {desttable} WHERE FileId = {fileid2}'
            rows_written = dts_import_instance.fn_fetch(query=query, fetch='fetchval', commit=False, close_conn=False)

            elapsed = self.file_util.seconds_to_hhmmss(round(time.perf_counter()-pstart, 2))
            log.info(f'End Insert, Elapsed: {elapsed}, Rows Written:{rows_written:,d}')
            if rows_read != rows_written:
                error = f'FileId2={fileid2}, Failed writting all rows...Read={rows_read:,d}, Written{rows_written:,d}'
                log.exception(error)
                raise ValueError(error)
                # exit('Process Failed...')

        query = self.app_config.get_parm_value(section=self.app_name,parm="update_file_status").format(self.table_name_files, 2, fileid2, fileid2)
        dts_import_instance.fn_execute(query=query, commit=True, close_conn=True)

        return


    def load_pending_ftp_files(self):
        
        future_list = []
        filestoload = ast.literal_eval(self.app_config.get_parm_value(section=self.app_name,parm="files_to_load"))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for arg in filestoload:
                future = executor.submit(self.load_pending_file, arg)
                future_list.append(future)

        for future in future_list:
            try:
                log.info(future.result())
            except Exception as e:
                log.exception(e)
                raise
        return
        

    def load_pending_file(self, filetoload):

        func = inspect.getframeinfo(inspect.currentframe()).function
        log.info(f"Func={func}, File to Load '{filetoload}', Start")
        start = time.perf_counter()

        dts_import_instance = Database.connect_dts_import(app_name=f'{self.app_name}_pid{filetoload}', log=log)
        pid = int(self.app_config.get_parm_value(section=self.app_name,parm="pid"))
        ftp_cfg = ast.literal_eval(self.app_config.get_parm_value(section=self.app_name,parm=f"file_{filetoload}_cfg"))

        ''' ftp.py '''
        ftp_port = 22
        ftp_host = ftp_cfg[0]
        ftp_user = ftp_cfg[1]
        ftp_cred = self.vault_util.get_secret(ftp_user)
        ftp_pswd = ftp_cred['password']                 
        ftp_path = ftp_cfg[2]
        ftp_mask = ftp_cfg[3]
        ftp_autoupdate = ftp_cfg[4]
        ftp_mode = ftp_cfg[5]
        filemask = ftp_cfg[6]

        ''' Connect to SFtp '''
        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path,log=log)            
        ''' Get Ftp files list '''
        ftp_filelist = sftp_conn.listdir(remotefolder=ftp_path, close_conn=True)
        for filename in ftp_filelist:
            ''' Find only files for this filecd '''
            # if fnmatch.fnmatch(filename, ftp_mask) and not self.check_for_file_loaded_sql(filename, pid):
            if fnmatch.fnmatch(filename, ftp_mask):
                query = self.app_config.get_parm_value(section=self.app_name,parm="check_file_processed").format(self.table_name_files, filename, pid, self.table_name_files)
                fileid = dts_import_instance.fn_fetch(query=query, fetch='fetchval', commit=False, close_conn=False)
                if not fileid:
                    fullname = os.path.join(self.temp_path, filename)
                    ''' Download the file '''
                    log.info(f'Downloading file...{filename}')
                    pstart = time.perf_counter()
                    '''Check if the sfpt is open by doing a listdir, if is not open the open the sftp'''
                    active_sftp_conn = sftp_conn.is_sftp_conn_active()
                    if not active_sftp_conn:
                        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=log)
                    sftp_conn.getfile(remotepath=f'{ftp_path}/{filename}',localpath=fullname,close_conn=False)
                    elapsed = self.file_util.seconds_to_hhmmss(round(time.perf_counter()-pstart, 2))
                    log.info(f'Downloaded Completed, Elapsed: {elapsed}')
                    ''' Process file '''
                    self.process_ftp_file(filetoload, fullname, pid)
        ''' Close Sftp Connection '''
        sftp_conn.close()
        ''' Close Db Connection '''
        dts_import_instance.fn_close()

        elapsed = self.file_util.seconds_to_hhmmss(round(time.perf_counter()-start, 2))
        return f'Func={func}, File {filetoload}, Elapsed: {elapsed}'


    def main(self):

        start = time.perf_counter() 
        rc = 0

        try:
            self.load_pending_ftp_files()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            elapsed = self.file_util.seconds_to_hhmmss(round(time.perf_counter()-start, 2))            
            log.info(f'---Process successfully completed, Elapsed: {elapsed}')
        finally:
            exit(rc)

obj = DQImport()
obj.main()

