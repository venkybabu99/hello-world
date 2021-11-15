# Initial Author: Leo Martinez
# Date Written: April 2021
# Overview: 1.  Address Std Batch
##############################################################################
import fnmatch
import os
import pandas as pd
import time
from pathlib import Path, PurePath
import json
import ast
import inspect
from configs import EnvironmentConfig
from database.Database import Database
from utils import get_logger
from utils.FileUtils import FileUtils
from vault import vault
from ftp import ftp
import stat
# ###################################

class AddrPlus_Batch(object):

    def __init__(self, log = None, app_name='AntiFraud'):
        self.log = log if log else get_logger('AddrPlus_Batch')
        self.app_name = app_name if app_name else 'AntiFraud'
        self.env_config = EnvironmentConfig()
        self.file_util = FileUtils(log=self.log)
        self.vault_util = vault(log=self.log)        
        # Default values from environment
        self.addrpls_server = self.env_config.get_addressplusbatch('addrpls_server')
        self.addrpls_user = self.env_config.get_addressplusbatch('addrpls_user')
        self.addrpls_path_incoming = self.file_util.RemovePrefixSufixSlash(slash='/',path=self.env_config.get_addressplusbatch('addrpls_path_incoming'))
        self.addrpls_path_outgoing = self.file_util.RemovePrefixSufixSlash(slash='/',path=self.env_config.get_addressplusbatch('addrpls_path_outgoing'))
        ftp_cred = self.vault_util.get_secret(self.addrpls_user)
        self.addrpls_pswd = ftp_cred['password']    
        # self.addrpls_pswd = 'asgs_one'
        self.localpath = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.localpath, createdir=True)
        # DEVLOPMENT ONLY
        # self.addrpls_server = 'ftp2.resftp.com'
        # self.addrpls_user = 'dtsdev'
        # self.addrpls_path_incoming = self.file_util.RemovePrefixSufixSlash(slash='/',path='/addressplus/in')
        # self.addrpls_path_outgoing = self.file_util.RemovePrefixSufixSlash(slash='/',path='/addressplus/out')
        # ftp_cred = self.vault_util.get_secret(self.addrpls_user)
        # self.addrpls_pswd = ftp_cred['password']    


    def AddressPlus_Push(self, df_headers, df_addr)-> str:

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        self.log.info(f'Func={func} Started...')

        list_header = df_headers.values.flatten()
        header_E = list_header[0]
        header_F = list_header[1]
        header_f_list = header_F.split('/')
        if header_E[:2] != 'E-':
            error = f"Invalid or no Header 'E-' row found in df_headers[0]=f'{header_E}'"
            raise ValueError(error)    
        if header_F[:2] != 'F-':
            error = f"Invalid or no Header 'F-' row found in df_headers[1]=f'{header_F}'"
            raise ValueError(error)    
        if len(header_f_list) != 7:
            error = 'Invalid or no Header "F-"'
            raise ValueError(error)    

        df_expected_nbr_of_columns = 11
        df_expected_columns = {'fips', 'pcl', 'pcl_seq', 'raw_sit_addr', 'raw_sit_city', 'raw_sit_st', 'raw_sit_zip', 'raw_mail_addr', 'raw_mail_city', 'raw_mail_st', 'raw_mail_zip'}
        if len(df_addr.columns) != df_expected_nbr_of_columns or not df_expected_columns.issubset(df_addr):
            expected_list = set(list(df_expected_columns))
            df_list = set(df_addr.columns.values.tolist())
            missing_columns = list(sorted(expected_list-df_list))
            error = f"Not all columns are in the dataframe\nTotal Columns Expected={df_expected_nbr_of_columns}, Columns in DF={len(df_addr.columns)}\nColumns in DF={sorted(df_addr.columns.values.tolist())}\nExpected columns={list(sorted(df_expected_columns))}\nMissing Columns={sorted(missing_columns)}"
            raise ValueError(error)    

        addrplus_outgoing_path = f'/{header_f_list[3]}/{header_f_list[4]}/{header_f_list[5]}'
        filename = f'{header_f_list[6]}'
        addrpls_queue =  f'{header_f_list[5]}'

        ftp_host = self.addrpls_server
        ftp_user = self.addrpls_user
        ftp_pswd = self.addrpls_pswd
        ftp_path = f'/{self.addrpls_path_incoming}/{addrpls_queue}/'
        ftp_port = 22
            
        pstart = time.perf_counter() 
        fullfilename = os.path.join(self.localpath, f'{filename}'.replace('.txt','.tmp'))
        self.log.info(f"Creating AddressPlus Input file... {fullfilename}")

        with open(fullfilename, 'w') as extr:
            extr.write(f"{header_E}\n")
            extr.write(f"{header_F}\n")
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

        tmpremotefilepath = f'{ftp_path}{filename}'.replace('.txt','.tmp')
        remotefilepath = f'{ftp_path}{filename}'

        pstart = time.perf_counter() 
        self.log.info(f"Ftp file... {fullfilename}")

        """ Connect to SFtp """
        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=self.log)

        sftp_conn.putfile(localpath=fullfilename, remotepath=tmpremotefilepath, close_conn=False)
        sftp_conn.rename(remotesrc=tmpremotefilepath, remotetrg=remotefilepath, close_conn=True)

        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
        self.log.info(f'Elapsed: {elapsed}')

        return addrplus_outgoing_path, filename, addrpls_queue


    def AddressPlus_Get(self, waitforfile: bool = False, addrplus_outgoing_path: str = None, filename_list: list = None, dbconn = None, desttable: str = None, truncate_table: bool = True )-> list:

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        self.log.info(f'Func={func} Started...')

        ftp_host = self.addrpls_server
        ftp_user = self.addrpls_user
        ftp_pswd = self.addrpls_pswd
        ftp_path = addrplus_outgoing_path
        ftp_port = 22

        max_attempts = 10
        attempt = 0

        if not desttable:
            desttable = '[dbo].[LM_TEST_ADDRPLUS]'

        self.define_addrpls_output(desttable=desttable, dbconn=dbconn, truncate_table=truncate_table)

        """ Remove None and empty values from the list """        
        filename_list = list(filter(str.split, list(filter(None,filename_list))))
        """ Dedup filename_list """
        filename_list = sorted(list(set(filename_list)))
        
        files_loaded = []
        while True:
            """ Connect to SFtp """
            sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, remotefolder=ftp_path, log=self.log)
            """ Get Ftp files list """
            ftp_filelist = sftp_conn.listdir_attr(remotefolder=ftp_path, close_conn=True)
            for filename in filename_list:
                for f in ftp_filelist:
                    if not stat.S_ISDIR(f.st_mode) and f.filename == filename and filename not in files_loaded:
                        attempt = 0
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
                        self.process_ftp_file(dbconn=dbconn, filename=fullname, desttable=desttable)
                        sftp_conn.removefile(remotepath=f'{ftp_path}/{f.filename}', close_conn=False)
                        self.file_util.RemoveFile(file=fullname)
                        files_loaded.append(filename)
                        break
                if len(filename_list) == len(files_loaded):
                    waitforfile = False
            """ Close Sftp Connection """
            sftp_conn.close()
            if not waitforfile:
                break
            attempt += 1
            if attempt > max_attempts:
                self.log.warning(f"Reached maximum number of attempts to find the file, max_attempts={max_attempts}")
                break  
            """ in seconds, sleep for 5 minutes """
            seconds=60*5
            self.log.info(f'Sleeping for {seconds} second(s)')
            time.sleep(seconds)     
            self.log.info(f'Attemp={attempt} of {max_attempts}...')

        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-start, 2))
        self.log.info(f'Elapsed: {elapsed}')

        return files_loaded


    def process_ftp_file(self, dbconn=None, filename: str = None, desttable: str = None):

        addrpls_widths = json.loads(self.env_config.get_addressplusbatch('addrpls_width'))
        addrpls_header = ast.literal_eval(self.env_config.get_addressplusbatch('addrpls_header'))

        if not filename:
            filename = "C:\services\ssis\QA\AddressStd\out\adc218220537.txt"

        if not dbconn:
            dbconn = Database.connect_dts_antifraud(app_name=f'{self.app_name}_AddrStd_Get', log=self.log)

        desttable_list = desttable.split('.')
        dest_schema = desttable_list[0].strip('][')
        dest_table = desttable_list[1].strip('][')        

        pstart = time.perf_counter() 
        self.log.info(f"Loading File '{filename}'")
        for df in pd.read_csv(filename, header = None, dtype = str, keep_default_na = False, delimiter = '~', usecols = addrpls_header, names = addrpls_header, index_col = False, iterator=True, chunksize= 100000):
            df.insert(0, 'FileName', PurePath(filename).name, True )
            dbconn.fn_to_sql(dest_schema=dest_schema, dest_table_name=dest_table, dataframe=df, chunk_limit=100000, truncate_table=False, commit=True, close_conn=False)
        elapsed = self.file_util.seconds_to_hhmmss(seconds=round(time.perf_counter()-pstart, 2))
        self.log.info(f'Elapsed: {elapsed}')   

        return


    def define_addrpls_output(self, desttable: str, dbconn, truncate_table: bool = True):
        
        if truncate_table:
            query = f'''IF OBJECT_ID('{desttable}','U') IS NOT NULL DROP TABLE {desttable};'''
            dbconn.fn_execute(query=query, commit = True, close_conn = False)

        query = f'''IF OBJECT_ID('{desttable}','U') IS NULL
            CREATE TABLE {desttable} (
                [FileName] [VARCHAR](100) NULL,
                [fips] [VARCHAR](5) NULL,
                [rec_type] [VARCHAR](2) NULL,
                [rec_type_occ] [VARCHAR](4) NULL,
                [pcl] [VARCHAR](60) NULL,
                [pcl_seq] [VARCHAR](3) NULL,
                [batch_date] [VARCHAR](8) NULL,
                [batch_seq] [VARCHAR](6) NULL,
                [isn] [VARCHAR](10) NULL,
                [building_seq] [VARCHAR](4) NULL,
                [clean_ind] [VARCHAR](1) NULL,
                [prop_ind] [VARCHAR](3) NULL,
                [hub_type] [VARCHAR](1) NULL,
                [csz_in] [VARCHAR](1) NULL,
                [raw_sit_typ] [VARCHAR](1) NULL,
                [raw_sit_addr] [VARCHAR](60) NULL,
                [raw_sit_city] [VARCHAR](40) NULL,
                [raw_sit_st] [VARCHAR](2) NULL,
                [raw_sit_zip] [VARCHAR](9) NULL,
                [raw_mail_addr] [VARCHAR](60) NULL,
                [raw_mail_city] [VARCHAR](40) NULL,
                [raw_mail_st] [VARCHAR](2) NULL,
                [raw_mail_zip] [VARCHAR](9) NULL,
                [sit_nbr] [VARCHAR](10) NULL,
                [sit_nbr_to] [VARCHAR](10) NULL,
                [sit_fract] [VARCHAR](10) NULL,
                [sit_predir] [VARCHAR](2) NULL,
                [sit_str] [VARCHAR](30) NULL,
                [sit_sfx] [VARCHAR](5) NULL,
                [sit_postdir] [VARCHAR](2) NULL,
                [sit_unit] [VARCHAR](10) NULL,
                [sit_addr] [VARCHAR](60) NULL,
                [sit_city] [VARCHAR](40) NULL,
                [sit_zip5] [VARCHAR](5) NULL,
                [sit_zip4] [VARCHAR](4) NULL,
                [sit_cr_rt] [VARCHAR](4) NULL,
                [sit_st] [VARCHAR](2) NULL,
                [sit_match] [VARCHAR](4) NULL,
                [sit_pfx] [VARCHAR](5) NULL,
                [sit_error_code] [VARCHAR](4) NULL,
                [sit_dpbc] [VARCHAR](2) NULL,
                [sit_ln_trav] [VARCHAR](4) NULL,
                [sit_ln_trav_ind] [VARCHAR](1) NULL,
                [sit_vanity_city] [VARCHAR](40) NULL,
                [sit_cass_flag] [VARCHAR](1) NULL,
                [sit_cens_tr] [VARCHAR](6) NULL,
                [sit_cens_blk] [VARCHAR](1) NULL,
                [sit_cens_blk2] [VARCHAR](2) NULL,
                [sit_cens_blk_sfx] [VARCHAR](1) NULL,
                [sit_latitude] [VARCHAR](10) NULL,
                [sit_longitude] [VARCHAR](11) NULL,
                [plotable] [VARCHAR](1) NULL,
                [sit_geo_match_cd] [VARCHAR](4) NULL,
                [sit_units_res] [VARCHAR](10) NULL,
                [sit_units_bus] [VARCHAR](10) NULL,
                [sit_units_unk] [VARCHAR](10) NULL,
                [sit_units_tot] [VARCHAR](10) NULL,
                [sit_cbsa_cd] [VARCHAR](5) NULL,
                [rdi] [VARCHAR](1) NULL,
                [mail_nbr] [VARCHAR](10) NULL,
                [mail_to] [VARCHAR](10) NULL,
                [mail_fract] [VARCHAR](10) NULL,
                [mail_predir] [VARCHAR](2) NULL,
                [mail_str] [VARCHAR](30) NULL,
                [mail_sfx] [VARCHAR](5) NULL,
                [mail_postdir] [VARCHAR](2) NULL,
                [mail_unit] [VARCHAR](10) NULL,
                [mail_addr] [VARCHAR](60) NULL,
                [mail_city] [VARCHAR](40) NULL,
                [mail_st] [VARCHAR](2) NULL,
                [mail_zip5] [VARCHAR](5) NULL,
                [mail_zip4] [VARCHAR](4) NULL,
                [mail_cr_rt] [VARCHAR](4) NULL,
                [mail_match] [VARCHAR](4) NULL,
                [mail_pfx] [VARCHAR](5) NULL,
                [mail_cntry] [VARCHAR](30) NULL,
                [mail_vanity_city] [VARCHAR](40) NULL,
                [mail_csz] [VARCHAR](60) NULL,
                [mail_last_line] [VARCHAR](60) NULL,
                [mail_error_code] [VARCHAR](4) NULL,
                [mail_dpbc] [VARCHAR](2) NULL,
                [mail_ln_trav] [VARCHAR](4) NULL,
                [mail_ln_trav_ind] [VARCHAR](1) NULL,
                [mail_cass_flag] [VARCHAR](1) NULL,
                [mail_cens_tr] [VARCHAR](6) NULL,
                [mail_cens_blk] [VARCHAR](1) NULL,
                [mail_cens_blk2] [VARCHAR](2) NULL,
                [mail_cens_blk_sfx] [VARCHAR](1) NULL,
                [mail_latitude] [VARCHAR](10) NULL,
                [mail_longitude] [VARCHAR](11) NULL,
                [mail_geo_match_cd] [VARCHAR](4) NULL,
                [mail_unit_type] [VARCHAR](10) NULL,
                [mail_high_unit] [VARCHAR](10) NULL,
                [mail_low_unit] [VARCHAR](10) NULL,
                [mail_pr_urb_cd] [VARCHAR](30) NULL,
                [map_sec] [VARCHAR](2) NULL,
                [map_twp] [VARCHAR](3) NULL,
                [map_rng] [VARCHAR](3) NULL,
                [map_sec_qtr] [VARCHAR](2) NULL,
                [map_sec_16] [VARCHAR](2) NULL,
                [tx_area] [VARCHAR](8) NULL,
                [map_link] [VARCHAR](20) NULL,
                [hm_exmpt] [VARCHAR](3) NULL,
                [msa] [VARCHAR](4) NULL,
                [abs_occ] [VARCHAR](1) NULL,
                [map_ref1] [VARCHAR](15) NULL,
                [map_pg1] [VARCHAR](9) NULL,
                [map_grid1] [VARCHAR](3) NULL,
                [ownr_buyr_addr-dpv_cd] [VARCHAR](2) NULL,
                [std_sit_hse1] [VARCHAR](10) NULL,
                [sit_dpv_confirm] [VARCHAR](2) NULL,
                [sit_dpv_cmra] [VARCHAR](2) NULL,
                [sit_dpv_footnote1] [VARCHAR](3) NULL,
                [sit_dpv_footnote2] [VARCHAR](3) NULL,
                [sit_lacslink_ind] [VARCHAR](2) NULL,
                [mail_dpv_confirm] [VARCHAR](2) NULL,
                [mail_dpv_cmra] [VARCHAR](2) NULL,
                [mail_dpv_footnote1] [VARCHAR](3) NULL,
                [mail_dpv_footnote2] [VARCHAR](3) NULL,
                [mail_lacslink_ind] [VARCHAR](2) NULL,
                [raw_mail_csz] [VARCHAR](60) NULL,
                [raw_mail_cntry] [VARCHAR](30) NULL,
                [sit_latitude_sds] [VARCHAR](10) NULL,
                [sit_longitude_sds] [VARCHAR](11) NULL,
                [sit_geo_match_cd_sds] [VARCHAR](4) NULL,
                [sit_dpv_vacant] [VARCHAR](1) NULL,
                [mail_dpv_vacant] [VARCHAR](1) NULL,
                [prior_pcl] [VARCHAR](60) NULL,
                [prior_pcl_seq] [VARCHAR](3) NULL)'''
        dbconn.fn_execute(query=query, commit = True, close_conn = False)
