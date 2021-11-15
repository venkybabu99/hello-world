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
from utils.FileUtils import FileUtils
# ###################################

log = get_logger('AntiFraud')

class AntiFraud:

    def __init__(self):
        self.app_name = 'AntiFraud'
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.today = datetime.now()
        self.file_util = FileUtils(log=log)


    def ParseNames(self):

        start = time.perf_counter()  
        func = inspect.getframeinfo(inspect.currentframe()).function        
        log.info(f'Func={func} Started...')

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name=f'{self.app_name}_ParseNames', log=log)

        # """ Pre-populate ParsedNames table for name parsing """
        # query = f'EXEC {self.app_config.get_parm_value(section=self.app_name,parm="sp_PrePopulateParsedNames")}'
        # dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        """ Get Names to parse """
        log.info('Getting Names to Parse...')
        pstart = time.perf_counter() 
        query = self.app_config.get_parm_value(section=self.app_name,parm="get_names_to_parse")
        df = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        num_rows = len(df)
        finish = time.perf_counter()
        elapsed = self.file_util.seconds_to_hhmmss(round(finish-pstart, 2))
        log.info(f'Elapsed: {elapsed} second(s), Rows {num_rows:,d} with {len(df.columns):,d} columns')

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

        # df.pop('RawName1')
        # df.pop('RawName2')
        # df.pop('RawName3')
        # df.pop('RawName4')

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

        """ Drop Temp Table """
        query = f"IF OBJECT_ID('{tmpparsedname}','U') IS NOT NULL DROP TABLE {tmpparsedname};"
        # dts_antrifraud_instance.fn_execute(query=query, commit=True, close_conn=False)

        dts_antrifraud_instance.fn_close()

        finish = time.perf_counter()
        return f'Func={func}, finished in {round(finish-start, 2)} second(s)'



    def main(self):

        start = time.perf_counter() 
        rc = 0        

        try:
            self.ParseNames()

        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            elapsed = self.file_util.seconds_to_hhmmss(round(finish-start, 2))
            log.info(f'---Process successfully completed, Elapsed: {elapsed}')
        finally:
            exit(rc)            


obj = AntiFraud()
obj.main()
