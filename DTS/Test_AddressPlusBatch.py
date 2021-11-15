# Initial Author: Leo Martinez
# Date Written: April 2021
# Overview: 1.  Address Std Push
#
# SQL Server=DTS
# SQL DB=AntiFraud  I/O
##############################################################################
import fnmatch
import os
import pathlib
import pandas as pd
import time
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
from configs.ConfigUtility import AppConfig
from configs import EnvironmentConfig
from database import Database
from utils import get_logger
from utils.EmailUtil import EmailUtil
from utils.FileUtils import FileUtils
from utils.ZipUtils import ZipUtils
from vault import vault
from ftp import ftp
import stat
# ###################################

class AddressStdBatch:

    def __init__(self, logger = None, app_name=None):
        self.log = logger if logger else get_logger('AddressStdBatch')
        self.app_name = app_name if app_name else 'AntiFraud'
        self.log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.file_util = FileUtils(log=self.log)
        self.vault_util = vault(log=self.log)        
        self.temp_path = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.temp_path, createdir=True)
        self.addrpls_server = self.env_config.get_addressplusbatch('addrpls_server')
        self.addrpls_user = self.env_config.get_addressplusbatch('addrpls_user')
        self.addrpls_path_incoming = self.RemoveEndSlashes(self.env_config.get_addressplusbatch('addrpls_path_incoming'))
        self.addrpls_path_outgoing = self.RemoveEndSlashes(self.env_config.get_addressplusbatch('addrpls_path_outgoing'))
        ftp_cred = self.vault_util.get_secret(self.addrpls_user)
        self.ftp_addrpls_pswd = ftp_cred['password']    
        self.addrpls_pswd = 'asgs_one'
        self.localpath = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.localpath, createdir=True)
        self.widths = json.loads(self.env_config.get_addressplusbatch('addrpls_width'))
        self.header = ast.literal_eval(self.env_config.get_addressplusbatch('addrpls_header'))

    def RemoveEndSlashes(self, path:str)->str:
        path_list = path.split('/')
        path = None
        for p in path_list:
            if p:
                path = f'{path}/{p}' if path else f'{p}'
        return path


    def AddressPlusPush(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        self.log.info(f'Func={func} Started...')

        addrpls_queue =  self.RemoveEndSlashes(self.app_config.get_parm_value(section=self.app_name,parm="addrpls_queue"))
        addrpls_email = self.app_config.get_parm_value(section=self.app_name,parm="addrpls_email")
        addrpls_ipaddr = self.app_config.get_parm_value(section=self.app_name,parm="addrpls_ipaddr")

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_AddrStd_Put', log=self.log)

        """ Get Pending Batch Id """
        self.log.info('Getting Pending AddrPlus Batches...')
        pstart = time.perf_counter() 
        query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdGetInputBatch")}'        
        df = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        num_rows = len(df)
        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
        self.log.info(f'Elapsed: {elapsed}, Rows {num_rows:,d} with {len(df.columns):,d} columns')

        if num_rows == 0:
            self.log.info("No rows to process...")
            return

        ftp_host = self.addrpls_server
        ftp_user = self.addrpls_user
        ftp_pswd = self.addrpls_pswd
        ftp_path = f'/{self.addrpls_path_incoming}/{addrpls_queue}/'
        ftp_port = 22

        startbtchs = time.perf_counter()
        self.log.info("Processing Batches...")
        for row_df in df.itertuples():

            startbtch = time.perf_counter()
            batchId = row_df.BatchId
            ExtrFileName = row_df.FileName
            self.log.info(f"Processing BatchId: {batchId}")
        
            """ Output Header Rows """
            # @OutputPath = 'asgstest/outgoing/anadt'
            query = f"SET NOCOUNT ON; EXEC [AddressPlusHeader] @Email = '{addrpls_email}', @IpAddress = '{addrpls_ipaddr}', @Username = '{self.addrpls_user}', @OutputPath = '{self.addrpls_path_outgoing}/{addrpls_queue}', @OutputFileName = '{ExtrFileName}'"
            df_headers = dts_antrifraud_instance.fn_populate_dataframe(query=query)
            
            pstart = time.perf_counter() 
            self.log.info("Getting all pending addresses...")
            query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdGetInput")} {batchId}'
            df_addr = dts_antrifraud_instance.fn_populate_dataframe(query=query, cnvrt_to_none=True)
            num_rows = len(df_addr)
            elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
            self.log.info(f'Elapsed: {elapsed}, Rows {num_rows:,d} with {len(df_addr.columns):,d} columns')
            if num_rows == 0:
                self.log.info("No rows to process...")
                continue

            pstart = time.perf_counter() 
            filename = os.path.join(self.temp_path, f'{ExtrFileName}.tmp')
            self.log.info(f"Creating AddressPlus Input file... {filename}")

            with open(filename, 'w') as extr:
                extr.write(f"{df_headers['Headers'][0]}\n")
                extr.write(f"{df_headers['Headers'][1]}\n")
                for index, row in df_addr.iterrows():
                    line = f"{row['fips']}"
                    line += f"~"                # rec_type
                    line += f"~"                # rec_type_occ
                    line += f"~{row['pcl']}"
                    line += f"~{row['pcl_seq']}"
                    line += f"~"                # batch_date
                    line += f"~"                # batch_seq
                    line += f"~"                # isn
                    line += f"~"                # building_seq
                    line += f"~"                # clean_ind
                    line += f"~"                # prop_ind
                    line += f"~"                # hub_type
                    line += f"~"                # csz_in
                    line += f"~"                # raw_sit_typ                    
                    line += f"~{row['raw_sit_addr']}"
                    line += f"~{row['raw_sit_city']}"
                    line += f"~{row['raw_sit_st']}"
                    line += f"~{row['raw_sit_zip']}"
                    line += f"~{row['raw_mail_addr']}"
                    line += f"~{row['raw_mail_city']}"
                    line += f"~{row['raw_mail_st']}"
                    line += f"~{row['raw_mail_zip']}"
                    line += f"~"                # sit_nbr
                    line += f"~"                # sit_nbr_to
                    line += f"~"                # sit_fract
                    line += f"~"                # sit_predir
                    line += f"~"                # sit_str
                    line += f"~"                # sit_sfx
                    line += f"~"                # sit_postdir
                    line += f"~"                # sit_unit
                    line += f"~"                # sit_addr
                    line += f"~"                # sit_city
                    line += f"~"                # sit_zip5
                    line += f"~"                # sit_zip4
                    line += f"~"                # sit_cr_rt
                    line += f"~"                # sit_st
                    line += f"~"                # sit_match
                    line += f"~"                # sit_pfx
                    line += f"~"                # sit_error_code
                    line += f"~"                # sit_dpbc
                    line += f"~"                # sit_ln_trav
                    line += f"~"                # sit_ln_trav_ind
                    line += f"~"                # sit_vanity_city
                    line += f"~"                # sit_cass_flag
                    line += f"~"                # cens_tr
                    line += f"~"                # cens_blk
                    line += f"~"                # cens_blk2
                    line += f"~"                # cens_blk_sfx
                    line += f"~"                # latitude
                    line += f"~"                # longitude
                    line += f"~"                # plotable
                    line += f"~"                # geo_match_cd
                    line += f"~"                # sit_unit_type
                    line += f"~"                # sit_high_unit
                    line += f"~"                # sit_low_unit
                    line += f"~"                # adv_unit_nbr_fwd
                    line += f"~"                # cbsa
                    line += f"~"                # rdi
                    line += f"~"                # mail_nbr
                    line += f"~"                # mail_to
                    line += f"~"                # mail_fract
                    line += f"~"                # mail_predir
                    line += f"~"                # mail_str
                    line += f"~"                # mail_sfx
                    line += f"~"                # mail_postdir
                    line += f"~"                # mail_unit
                    line += f"~"                # mail_addr
                    line += f"~"                # mail_city
                    line += f"~"                # mail_st
                    line += f"~"                # mail_zip5
                    line += f"~"                # mail_zip4
                    line += f"~"                # mail_cr_rt
                    line += f"~"                # mail_match
                    line += f"~"                # mail_pfx
                    line += f"~"                # mail_cntry
                    line += f"~"                # mail_vanity_city
                    line += f"~"                # mail_csz
                    line += f"~"                # mail_last_line
                    line += f"~"                # mail_error_code
                    line += f"~"                # mail_dpbc
                    line += f"~"                # mail_ln_trav
                    line += f"~"                # mail_ln_trav_ind
                    line += f"~"                # mail_cass_flag
                    line += f"~"                # mail_cens_tr
                    line += f"~"                # mail_cens_blk
                    line += f"~"                # mail_cens_blk2
                    line += f"~"                # mail_cens_blk_sfx
                    line += f"~"                # mail_latitude
                    line += f"~"                # mail_longitude
                    line += f"~"                # mail_geo_match_cd
                    line += f"~"                # mail_unit_type
                    line += f"~"                # mail_high_unit
                    line += f"~"                # mail_low_unit
                    line += f"~"                # mail_pr_urb_cd
                    line += f"~"                # map_sec
                    line += f"~"                # map_twp
                    line += f"~"                # map_rng
                    line += f"~"                # map_sec_qtr
                    line += f"~"                # map_sec_16
                    line += f"~"                # tx_area
                    line += f"~"                # map_link
                    line += f"~"                # hm_exmpt
                    line += f"~"                # msa
                    line += f"~"                # abs_occ
                    line += f"~"                # map_ref1
                    line += f"~"                # map_pg1
                    line += f"~"                # map_grid1
                    line += f"~"                # ownr_buyr_addr_dpv_cd
                    line += f"~"                # std_sit_hse1
                    line += f"~"                # sit_dpv_confirm
                    line += f"~"                # sit_dpv_cmra
                    line += f"~"                # sit_dpv_footnote1
                    line += f"~"                # sit_dpv_footnote2
                    line += f"~"                # sit_lacslink_ind
                    line += f"~"                # mail_dpv_confirm
                    line += f"~"                # mail_dpv_cmra
                    line += f"~"                # mail_dpv_footnote1
                    line += f"~"                # mail_dpv_footnote2
                    line += f"~"                # mail_lacslink_ind
                    line += f"~"                # raw_mail_csz
                    line += f"~"                # raw_mail_country
                    line += f"~"                # sit_latitude_sds
                    line += f"~"                # sit_longitude_sds
                    line += f"~"                # sit_geo_match_cd_sds
                    line += f"~"                # sit_dpv_vacant
                    line += f"~"                # mail_dpv_vacant
                    line += f"~"                # prior_pcl
                    line += f"~"                # prior_pcl_seq
                    line += '\n'
                    extr.write(line)
                extr.close()

            elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
            self.log.info(f'Elapsed: {elapsed}')

            tmpremotefilepath = f'{ftp_path}{ExtrFileName}.tmp'
            remotefilepath = f'{ftp_path}{ExtrFileName}.txt'

            pstart = time.perf_counter() 
            self.log.info(f"Ftp file... {filename}")

            """ Connect to SFtp """
            sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=self.log)

            sftp_conn.putfile(localpath=filename, remotepath=tmpremotefilepath, close_conn=False)
            sftp_conn.rename(remotesrc=tmpremotefilepath, remotetrg=remotefilepath, close_conn=True)

            elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
            self.log.info(f'Elapsed: {elapsed}')

            elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-startbtch, 2))
            self.log.info(f'Completed batch process, Batch Id={batchId},  Elapsed: {elapsed}')

        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-startbtchs, 2))
        self.log.info(f'Completed processing batches... Elapsed: {elapsed}')

        dts_antrifraud_instance.fn_close()

        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-start, 2))
        return f'Func={func}, Elapsed: {elapsed}'


    def AddressPlusGet(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        self.log.info(f'Func={func} Started...')

        addrpls_queue =  self.RemoveEndSlashes(self.app_config.get_parm_value(section=self.app_name,parm="addrpls_queue"))
        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_AddrStd_Get', log=self.log)

        ftp_host = self.addrpls_server
        ftp_user = self.addrpls_user
        ftp_pswd = self.addrpls_pswd
        ftp_path = f'/{self.addrpls_path_outgoing}/{addrpls_queue}/'
        ftp_port = 22

        """ Get Pending Batch Id """
        self.log.info('Getting Pending AddrPlus Batches...')
        pstart = time.perf_counter() 
        query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdGetPending")}'        
        df = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        num_rows = len(df)
        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
        self.log.info(f'Elapsed: {elapsed}, Rows {num_rows:,d} with {len(df.columns):,d} columns')

        if num_rows == 0:
            self.log.info("No files to load...")
            return

        startload = time.perf_counter() 

        """ Connect to SFtp """
        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=self.log)
        """ Get Ftp files list """
        ftp_filelist = sftp_conn.listdir_attr(remotefolder=ftp_path, close_conn=True)
        for f in ftp_filelist:
            if not stat.S_ISDIR(f.st_mode) and fnmatch.fnmatch(f.filename, '*.txt') and Path(f.filename).stem in df.values:
                fullname = os.path.join(self.localpath, f.filename)
                """ Download the file """
                self.log.info(f'Downloading file...{f.filename}')
                pstart = time.perf_counter() 
                """Check if the sfpt is open by doing a listdir, if is not open the open the sftp"""
                active_sftp_conn = sftp_conn.is_sftp_conn_active()
                if not active_sftp_conn:
                    sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=self.log)
                sftp_conn.getfile(remotepath=f'{ftp_path}/{f.filename}', localpath=fullname, close_conn=False)
                elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
                self.log.info(f'Downloaded Completed, Elapsed: {elapsed}')
                """ Process file """
                self.process_ftp_file(dbconn=dts_antrifraud_instance, filename=fullname)
                sftp_conn.removefile(remotepath=f'{ftp_path}/{f.filename}', close_conn=False)
                self.file_util.RemoveFile(file=fullname)
        """ Close Sftp Connection """
        sftp_conn.close()
        dts_antrifraud_instance.fn_close()

        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-startload, 2))
        self.log.info(f'Completed Loading all files, Elapsed: {elapsed}')

        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-start, 2))
        self.log.info(f'Elapsed: {elapsed}')


    def process_ftp_file(self, dbconn=None, filename: str = None):

        header = ast.literal_eval(self.app_config.get_parm_value(section=self.app_name,parm='addrpls_header'))
        if not header:
            header = self.header
        if not filename:
            filename = "C:\services\ssis\QA\AddressStd\out\QA_AFraud_20211031093138.txt"

        if not dbconn:
            dbconn = Database.connect_dts_antifraud(app_name=f'{self.app_name}_AddrStd_Get', log=self.log)

        """ Get FileId """
        query = f"SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm='sp_GetFileInfo')} 8, '{filename}'"
        df_fi = dbconn.fn_populate_dataframe(query=query)
        num_rows = len(df_fi)
        if num_rows == 0:
            error = f'Unable to get FileId, query={query}'
            self.log.exception(error)
            raise ValueError(error)    
        
        fileid = str(df_fi['FileId'].values[0])

        desttable = self.app_config.get_parm_value(section=self.app_name,parm="addrstdstaging_table")
        query = f"TRUNCATE TABLE {desttable}"
        dbconn.fn_execute(query=query, commit=True, close_conn=False)

        desttable_list = desttable.split('.')
        dest_schema = desttable_list[0].strip('][')
        dest_table = desttable_list[1].strip('][')        

        pstart = time.perf_counter() 
        self.log.info("Loading File '{filename}'")
        for df in pd.read_csv(filename, header = None, dtype = str, keep_default_na = False, delimiter = '~', usecols = header, names = header, index_col = False, iterator=True, chunksize= 100000):
            df.insert(0, 'FileId', fileid, True )
            dbconn.fn_to_sql(dest_schema=dest_schema, dest_table_name=dest_table, dataframe=df, chunk_limit=100000, truncate_table=False, commit=True, close_conn=False)
        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
        self.log.info(f'Elapsed: {elapsed}')        

        """ Update inserted rows AddressStd """
        pstart = time.perf_counter() 
        self.log.info("Updating AddressStd Table...")
        query = f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_AddressStdMatchOutput")}'
        dbconn.fn_execute(query=query, commit=True, close_conn=False)

        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
        self.log.info(f'Elapsed: {elapsed}')        

        query=f'SET NOCOUNT ON; EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_SetFileStatus")} {fileid}, 8'
        dbconn.fn_execute(query=query, commit=True, close_conn=False)


    def main(self):

        start = time.perf_counter() 
        rc = 0        

        try:
            # self.AddressPlusPush()
            self.AddressPlusGet()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[self.log.handlers[0].baseFilename])
            rc = 1
        else:
            elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-start, 2))
            self.log.info(f'---Process successfully completed, Elapsed: {elapsed}')
        finally:
            exit(rc)            


obj = AddressStdBatch(app_name='AntiFraud')
obj.main()
