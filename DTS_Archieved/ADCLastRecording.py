# Author: Marcio
# Date: May 2021
# Overview:  Python code to extract ADCLastRecording
# - Importing Configurations
# - Connect to SQL Server
# - Error Handling
# - Create Excel for ADCLatestRecording
#
# SQL Server=DTS
# SQL DB=readme
#############################################################################
import os
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

log = get_logger('ADCLastRecording')

class ADCLastRecording:

    def __init__(self):

        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)        
        self.adc_readme_instance = Database.connect_dts_readme(app_name=self.app_name, log=log)
        self.tempfolder = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.tempfolder, createdir=True)
        

    def extract_ADCLastRecording(self):

        query = self.app_config.get_parm_value(section=self.app_name,parm="get_ADCLastRecording_Records")
        # Setup output file name
        coprofile = f'COPRO_{time.strftime("%Y%m%d")}.TXT'
        tempfiletxt= os.path.join(self.tempfolder, coprofile)

        df_extr = self.adc_readme_instance.fn_populate_dataframe(query=query)
        num_rows = len(df_extr)
        log.info(f'Rows Read:{num_rows}, # of Columns:{len(df_extr.columns)}')
        self.adc_readme_instance.fn_close()

        log.info("Writing dataframe content to file")
        with open(tempfiletxt, 'w') as extr:
            for index, row in df_extr.iterrows():    
                line = f"{row['FIPS'][0:5]:<5}"
                line += f"{row['DeedCategory'][0:3]:<3}"
                line += f"{row['LastRecording'][0:8]:<8}"      
                line += '\n'
                extr.write(line)
            extr.close()                              
       
        # Setup sftp credentials
        log.info("Placing file on ftp")        
        ftp_port = 22
        ftp_host = self.app_config.get_parm_value(section=self.app_name,parm="ftp_host")
        ftp_path = self.app_config.get_parm_value(section=self.app_name,parm="ftp_path")
        ftp_user = self.app_config.get_parm_value(section=self.app_name,parm="ftp_user")
        cred = self.vault_util.get_secret(ftp_user)        
        ftp_pswd = cred['password'] 

        sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, log=log)
        sftp_conn.putfile(localpath = tempfiletxt, remotepath = os.path.join(ftp_path, coprofile), close_conn = True)  


    def main(self):
        
        start = time.perf_counter() 
        rc = 0        
        try:
            self.extract_ADCLastRecording()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)   


obj = ADCLastRecording()
obj.main()

