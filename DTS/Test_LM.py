from database import Database
from utils.AddrPlus_Batch import *
##############################################################################
from glob import glob
from datetime import datetime
import json
import ast
import inspect
#
from configs.ConfigUtility import AppConfig
from configs import EnvironmentConfig
from database import Database
from utils import get_logger
# ###################################

log = get_logger('TEST_ADDR')

class TEST:

    def __init__(self):
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.today = datetime.now()
        self.addrplus = AddrPlus_Batch(log=log, app_name='TEST') 

    def DoGet(self):

        dts_antrifraud_instance = Database.connect_dts_antifraud(app_name='TEST', log=log)

        """ Output Header Rows """
        # @OutputPath = 'asgstest/outgoing/anadt'
        query = f"SET NOCOUNT ON; EXEC [AddressPlusHeader] @Email = 'lmartinez@corelogic.com', @IpAddress = 'test_ip_address', @Username = 'dtsenv', @OutputPath = 'addressplus/out/testqueue', @OutputFileName = 'qa_test_2'"
        df_headers = dts_antrifraud_instance.fn_populate_dataframe(query=query)
        query = f"SET NOCOUNT ON; SELECT TOP 100 fips, pcl, pcl_seq, [raw_sit_addr], [raw_sit_city], [raw_sit_st], [raw_sit_zip], [raw_mail_addr], [raw_mail_city], [raw_mail_st], [raw_mail_zip] FROM [AntiFraud].[dbo].[LM_TEST_ADDRPLUS] WHERE filename = 'QA_AFraud_1.txt'"
        df_addr = dts_antrifraud_instance.fn_populate_dataframe(query=query, cnvrt_to_none=True)

        addrplus_outgoing_path, filename, queue = self.addrplus.AddressPlus_Push(df_headers=df_headers, df_addr=df_addr)
        
        # file_loaded = self.addrplus.AddressPlus_Get(waitforfile=True, addrplus_outgoing_path=addrplus_outgoing_path, filename_list=['QA_AFraud_1.txt','',None,'QA_AFraud_4.txt','QA_AFraud_3.txt','QA_AFraud_3.txt','QA_AFraud_2.txt'], dbconn=dts_antrifraud_instance, desttable=None, truncate_table=True)
        file_loaded = self.addrplus.AddressPlus_Get(waitforfile=True, addrplus_outgoing_path=addrplus_outgoing_path, filename_list=filename.split("^"), dbconn=dts_antrifraud_instance, desttable='[dbo].[LM_TEST_ADDRPLUS_2]', truncate_table=True)

        dts_antrifraud_instance.fn_close()
        print(file_loaded)

    def main(self):

        try:
            self.DoGet()
        except Exception as e:
            log.exception(e, exc_info=e)


obj = TEST()
obj.main()



