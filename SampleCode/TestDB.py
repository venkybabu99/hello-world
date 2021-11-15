from configs.ConfigUtility import AppConfig
from utils import get_logger
# from utils import WriteCSVfile
from configs import EnvironmentConfig
from database import Database
import datetime
from pandas import DataFrame

log = get_logger('TestDB')

class TestDB:

    def __init__(self):
        log.info('TestDB object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()

    def GetExtractCount(self):
        try:
            #Connect to DB
            dts_adc_instance = Database.connect_dts_adc("sqlserver")

            log.info("hier")

            extractcount = 10 # dts_adc_instance.fn_fetch("EXEC [dbo].[GetExtractCount]", 'fetchval', False, False)
            log.info(str(extractcount)+' records to process...')

            batch = 0
            NextBuffer = False
            while extractcount > 0:

                #Assign file datr and batch number
                FileDate = str(datetime.datetime.now()).replace("-", '').replace(":", "").replace(".", "").replace(" ", "_")[0:8]
                batch = batch+1

                #Assign filename
                filename = r"D:\temp\TestDB\adc"+FileDate+"e"+str(batch)+".txt"
                # dts_adc_instance.fn_execute("EXEC [dbo].[PrepTestDB]", True, False)

                #extract = dts_adc_instance.fn_fetch("EXEC [dbo].[GetTestDB]", 'fetchmany', False, False)
                extract = dts_adc_instance.fn_fetch("EXEC [dbo].[GetAdcExtract_Anton]", fetch='fetchmany', commit=False, close_conn=False, get_next_buffer=NextBuffer, buffer_size = 2)

                # Create txt file with output from GetTestDB
                #WriteCSVfile(filename,extract)
                #extract.to_csv()
                print("****")
                print(*extract, sep = "\n")

                extractcount = dts_adc_instance.fn_fetch("EXEC [dbo].[GetExtractCount]", 'fetchval', False, False)
                #log.info(str(extractcount)+' records to process...')
                NextBuffer = True
            #Close DB connection
            dts_adc_instance.fn_close()

        except Exception as inst:
            print("*** Error Information ***")
            print(type(inst))    # the exception instance
            print(inst.args)     # arguments stored in .args
            print(inst)          # __str__ allows args to be printed directly,

    def main(self):
        self.GetExtractCount()


obj = TestDB()
obj.main()




        
