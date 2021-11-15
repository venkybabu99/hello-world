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
        
        # addrow['latitude'] = siteraw['Latitude']
        latitude_split = siteraw['Latitude'].split('.')
        addrow['latitude'] = f'{latitude_split[0]}.{latitude_split[1][:6]}'

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

        # addrow['mail_latitude'] = mailraw['Latitude']
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


    def GatherLatestData(self):

        """ Address Standarization Input """
        self.AddrStandarized()

        return


    def main(self):

        start = time.perf_counter() 
        rc = 0        

        try:
            """ SC Gather Latest Data """
            self.GatherLatestData()

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
