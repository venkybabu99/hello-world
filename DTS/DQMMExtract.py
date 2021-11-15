# Initial Author: Leo Martinez
# Date Written: Sept 2021
# Overview: 1. Rewrite DQMMExtrat SSIS Package to Python
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=countyapnprofile   I
##############################################################################
# import sys
import datetime
import os
import time
# import ast
from pathlib import Path
import os
import pandas as pd
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

log = get_logger('DQMMExtract')

class DQMMExtract:

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
        self.dts_countyapnprofile_instance = Database.connect_dts_countyapnprofile(app_name=self.app_name, log=log)        
        self.file_util.CheckDirExists(folder=self.temp_path, createdir=True)


    def GenerateMMExtractFile(self):
        
        pstart = time.perf_counter() 
        log.info('Exec stored proced ExtractMM...')
        query = "EXEC [dbo].[ExtractMM]"
        df = self.dts_countyapnprofile_instance.fn_populate_dataframe(query=query, cnvrt_to_none=True)

        df['MM_MUNI_CODE']= df['MM_MUNI_CODE'].fillna(' ')
        df['MM_STATE_CODE']= df['MM_STATE_CODE'].fillna(' ')
        df['MM_MUNI_NAME']= df['MM_MUNI_NAME'].fillna(' ')
        df['MM_COUNTY_NAME']= df['MM_COUNTY_NAME'].fillna(' ')
        df['MM_PARCEL_TYPE_CODE']= df['MM_PARCEL_TYPE_CODE'].fillna(' ')
        df['MM_ASSR_AVAIL_FLAG']= df['MM_ASSR_AVAIL_FLAG'].fillna(' ')
        df['MM_RCDR_AVAIL_FLAG']= df['MM_RCDR_AVAIL_FLAG'].fillna(' ')
        df['MM_AS_AVAIL_FLAG']= df['MM_AS_AVAIL_FLAG'].fillna(' ')
        df['MM_NOD_AVAIL_FLAG']= df['MM_NOD_AVAIL_FLAG'].fillna(' ')
        df['MM_NOT_AVAIL_FLAG']= df['MM_NOT_AVAIL_FLAG'].fillna(' ')
        df['MM_NON_DISCLOSURE_FLAG']= df['MM_NON_DISCLOSURE_FLAG'].fillna(' ')
        df['MM_PARCEL_DESC_1']= df['MM_PARCEL_DESC_1'].fillna(' ')
        df['MM_PARCEL_DESC_2']= df['MM_PARCEL_DESC_2'].fillna(' ')
        df['MM_PARCEL_DESC_3']= df['MM_PARCEL_DESC_3'].fillna(' ')
        df['MM_PARCEL_DESC_4']= df['MM_PARCEL_DESC_4'].fillna(' ')
        df['MM_PARCEL_DESC_5']= df['MM_PARCEL_DESC_5'].fillna(' ')
        df['MM_PARCEL_DESC_6']= df['MM_PARCEL_DESC_6'].fillna(' ')
        df['MM_PARCEL_DESC_7']= df['MM_PARCEL_DESC_7'].fillna(' ')
        df['MM_PARCEL_DESC_8']= df['MM_PARCEL_DESC_8'].fillna(' ')
        df['MM_PARCEL_DESC_9']= df['MM_PARCEL_DESC_9'].fillna(' ')
        df['MM_PARCEL_DESC_10']= df['MM_PARCEL_DESC_10'].fillna(' ')
        df['MM_UPDATE_FLAG']= df['MM_UPDATE_FLAG'].fillna(' ')
        df['MORTGAGES_MISSING_FLAG']= df['MORTGAGES_MISSING_FLAG'].fillna(' ')
        df['MM_MULTIPLE_APN_FORMAT']= df['MM_MULTIPLE_APN_FORMAT'].fillna(' ')
                
        rows_select = len(df)
        total_cols = len(df.columns)
        finish = time.perf_counter() 
        log.info(f'Elapsed: {round(finish-pstart, 2)} second(s), Rows Read:{rows_select}, # of Columns:{total_cols}')
        self.dts_countyapnprofile_instance.fn_close()

        ''' Do a group by on MM_SCM_ID column and get the counts '''
        df_groupby = df.groupby(['MM_SCM_ID']).size().reset_index()
        df_groupby.columns.values[1] = 'Count'

        zip_pswd = self.app_config.get_parm_value(section=self.app_name,parm="zip_pswd").encode()
        localpath = self.app_config.get_parm_value(section=self.app_name,parm="localpath")
        folderfnd = self.file_util.CheckDirExists(folder=localpath, createdir=True)
        if not folderfnd:
            error = f"Folder not found '{localpath}', processed failed..."
            log.exception(error)
            raise ValueError(error)
            # exit(1)

        DQMCFile = f'MM_{str(datetime.datetime.now())[0:10].replace("-", "")}_1.txt'
        FullDQMCFile = os.path.join(localpath, DQMCFile)
        DQMCStats = Path(FullDQMCFile).stem + "STATS.txt"
        FullDQMCStats = os.path.join(localpath, DQMCStats)
        DQMCZipfile = Path(DQMCFile).stem + '.zip'

        ''' Create detail file '''
        with open(FullDQMCFile, 'w') as extr:
            for index, row in df.iterrows():    
                line = f"{row['MM_SCM_ID']:05}"
                line += row['MM_FIPS_STATE_CODE'].rjust(5,"0")
                line += row['MM_FIPS_MUNI_CODE'].rjust(5,"0")
                line += row['MM_FIPS_COUNTY_CODE'].rjust(5,"0")
                line += f"{row['MM_MUNI_CODE'][0:5]:<5}"
                line += f"{row['MM_STATE_CODE'][0:2]:<2}"
                line += f"{row['MM_MUNI_NAME'][0:24]:<24}"
                line += f"{row['MM_COUNTY_NAME'][0:24]:<24}"
                line += '  ' if row['MM_PARCEL_LENGTH_1'] is None else f"{int(row['MM_PARCEL_LENGTH_1']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_2'] is None else f"{int(row['MM_PARCEL_LENGTH_2']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_3'] is None else f"{int(row['MM_PARCEL_LENGTH_3']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_4'] is None else f"{int(row['MM_PARCEL_LENGTH_4']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_5'] is None else f"{int(row['MM_PARCEL_LENGTH_5']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_6'] is None else f"{int(row['MM_PARCEL_LENGTH_6']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_7'] is None else f"{int(row['MM_PARCEL_LENGTH_7']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_8'] is None else f"{int(row['MM_PARCEL_LENGTH_8']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_9'] is None else f"{int(row['MM_PARCEL_LENGTH_9']):02}"                    
                line += '  ' if row['MM_PARCEL_LENGTH_10'] is None else f"{int(row['MM_PARCEL_LENGTH_10']):02}"                    
                line += f"{row['MM_PARCEL_TYPE_CODE'][0:3]:<3}"
                line += '  ' if row['MM_NUM_PARCEL_CHARS'] is None else f"{int(row['MM_NUM_PARCEL_CHARS']):02}"                    
                line += f"{row['MM_ASSR_AVAIL_FLAG'][0:1]:<1}"
                line += f"{row['MM_RCDR_AVAIL_FLAG'][0:1]:<1}"
                line += f"{row['MM_AS_AVAIL_FLAG'][0:1]:<1}"
                line += f"{row['MM_NOD_AVAIL_FLAG'][0:1]:<1}"
                line += f"{row['MM_NOT_AVAIL_FLAG'][0:1]:<1}"
                line += f"{row['MM_NON_DISCLOSURE_FLAG'][0:1]:<1}"
                line += f"{row['MM_PARCEL_DESC_1'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_2'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_3'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_4'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_5'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_6'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_7'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_8'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_9'][0:18]:<18}"
                line += f"{row['MM_PARCEL_DESC_10'][0:18]:<18}"
                line += '  ' if row['MM_BOOK_LENGTH'] is None else f"{int(row['MM_BOOK_LENGTH']):02}"                    
                line += '  ' if row['MM_PAGE_LENGTH'] is None else f"{int(row['MM_PAGE_LENGTH']):02}"                    
                line += f"{row['MM_UPDATE_FLAG'][0:1]:<1}"
                line += f"{row['MORTGAGES_MISSING_FLAG'][0:1]:<1}"
                line += f"{row['MM_MULTIPLE_APN_FORMAT'][0:1]:<1}"
                line += '\n'
                extr.write(line)
            extr.close()              
        
        ''' Create stats file '''
        with open(FullDQMCStats, 'w') as extr:
            line = f'0,{rows_select},5'
            line += '\n'
            extr.write(line)
            for index, row in df_groupby.iterrows():    
                line = f"{row['MM_SCM_ID']},{row['Count']},5"
                line += '\n'
                extr.write(line)
            extr.close()              

        ''' Zip file '''
        zipfile = os.path.join(localpath, DQMCZipfile)
        self.zip_util.Compress(srcefile=FullDQMCFile, srcefilepath=None, destfile=zipfile, password=zip_pswd, compression_level=5, deletesrce=True)

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
            self.GenerateMMExtractFile()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)


obj = DQMMExtract()
obj.main()
