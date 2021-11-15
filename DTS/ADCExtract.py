# Initial Author: Danny Skandrani
# Date Written: May 2021
#
# Python script to replace ADCExtract SSIS Package.
# 1.Execute GetExtractCount stored procedure.
# 2.Execute PrepAdcExtract stored procedure.
# 3.Create txt file with output from GetAdcExtract.
# 4.Zip output file.
# 5.Execute UpdateExportTrackingBR stored procedure.
# The steps above will continue to occur in batches of 500,000
# until GetExtractCount returns 0.
#
# SQL Server=DTS
# SQL DB=ADC    I/O
# ############################################################################
import datetime
import os
import pandas
import time
from pathlib import Path
# ###################################
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

log = get_logger('ADCExtract')

class ADCExtract:

    def __init__(self):
        self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
        log.info(f'{self.app_name} object created...')
        self.env_config = EnvironmentConfig()
        self.app_config = AppConfig()
        self.zip_util = ZipUtils(log=log)
        self.email_util = EmailUtil(log=log)
        self.file_util = FileUtils(log=log)
        self.vault_util = vault(log=log)        
        self.localpath = Path(self.env_config.local_path()).joinpath(self.app_name)
        self.file_util.CheckDirExists(folder=self.localpath, createdir=True)

    def Extract(self):
        #Connect to DB
        dts_adc_instance = Database.connect_dts_adc(app_name=self.app_name, log=log)

        #Get inital extract count
        extractcount = dts_adc_instance.fn_fetch(query="EXEC [dbo].[GetExtractCount]", fetch='fetchval', commit=False, close_conn=False)
        log.info(f'{str(extractcount)} records to process...')
        if extractcount > 0:
            # Get ftp parms
            ftp_port = 22
            ftp_host = self.app_config.get_parm_value(section=self.app_name,parm="ftp_host")
            ftp_path = self.app_config.get_parm_value(section=self.app_name,parm="ftp_path")
            ftp_user = self.app_config.get_parm_value(section=self.app_name,parm="ftp_user")                        
            cred = self.vault_util.get_secret(ftp_user)
            ftp_pswd = cred['password'] 

            batch = 0
            while extractcount > 0:
                #Assign file datr and batch number
                FileDate = str(datetime.datetime.now()).replace("-", '').replace(":", "").replace(".", "").replace(" ", "_")[0:8]
                batch = batch + 1
                #Assign filename

                filename=f'{os.path.join(self.localpath)}\\adc{FileDate}e{str(batch)}.txt'
                log.info(f'Processing batch {str(batch)} to filename {filename}')
                dts_adc_instance.fn_execute(query="EXEC [dbo].[PrepAdcExtract]", commit=True, close_conn=False)
                #Create txt file with output from GetAdcExtract
                buffersize = 100000
                with open(filename,'w') as extr:
                    df=dts_adc_instance.fn_fetch(query="EXEC [dbo].[GetAdcExtract]", fetch='fetchmany', commit=False, close_conn=False,
                        get_next_buffer=False,buffer_size=buffersize, return_dataframe = True)
                    while df.shape[0] > 0:
                        df = df.fillna(' ')   
                        for index, row in df.iterrows():  
                            line  = f"{row['1']:5}" # 1
                            line += f"{row['2']:3}" # 6
                            line += f"{row['3']:1}" # 9
                            line += f"{row['4']:4}" # 10
                            line += f"{row['5']:20}" # 14
                            line += f"{row['6']:10}" # 34
                            line += f"{row['7']:10}" # 44
                            line += f"{row['8']:12}" # 54
                            line += f"{row['9']:8}" # 66
                            line += f"{row['10']:60}" # 74
                            line += f"{row['11']:15}" # 134
                            line += f"{row['12']:60}" # 149
                            line += f"{row['13']:30}" # 209
                            line += f"{row['14']:30}" # 239
                            line += f"{row['15']:4}" # 269
                            line += f"{row['16']:60}" # 273
                            line += f"{row['17']:40}" # 333
                            line += f"{row['18']:2}" # 373
                            line += f"{row['19']:5}" # 375
                            line += f"{row['20']:4}" # 380
                            line += f"{row['21']:60}" # 384
                            line += f"{row['22']:8}" # 444
                            line += f"{row['23']:8}" # 452
                            line += f"{row['24']:20}" # 460
                            line += f"{row['25']:9}" # 480
                            line += f"{row['26']:8}" # 489
                            line += f"{row['27']:13}" # 497
                            line += f"{row['28']:40}" # 510
                            line += f"{row['29']:2}" # 550
                            line += f"{row['30']:8}" # 552
                            line += f"{row['31']:8}" # 560
                            line += f"{row['32']:20}" # 568
                            line += f"{row['33']:10}" # 588
                            line += f"{row['34']:10}" # 598
                            line += f"{row['35']:100}" # 608
                            line += f"{row['36']:20}" # 708
                            line += f"{row['37']:30}" # 728
                            line += f"{row['38']:30}" # 758
                            line += f"{row['39']:30}" # 788
                            line += f"{row['40']:4}" # 818
                            line += f"{row['41']:9}" # 822
                            line += f"{row['42']:30}" # 831
                            line += f"{row['43']:30}" # 861
                            line += f"{row['44']:30}" # 891
                            line += f"{row['45']:4}" # 921
                            line += f"{row['46']:8}" # 925
                            line += f"{row['47']:60}" # 933
                            line += f"{row['48']:40}" # 993
                            line += f"{row['49']:2}" # 1033
                            line += f"{row['50']:5}" # 1035
                            line += f"{row['51']:4}" # 1040
                            line += f"{row['52']:9}" # 1044
                            line += f"{row['53']:60}" # 1053
                            line += f"{row['54']:10}" # 1113
                            line += f"{row['55']:1}" # 1123
                            line += f"{row['56']:1}" # 1124
                            line += f"{row['57']:1}" # 1125
                            line += f"{row['58']:20}" # 1126
                            line += f"{row['59']:2}" # 1146
                            line += f"{row['60']:8}" # 1148
                            line += f"{row['61']:8}" # 1156
                            line += f"{row['62']:60}" # 1164
                            line += f"{row['63']:1}" # 1224
                            line += f"{row['64']:1}" # 1225
                            line += f"{row['65']:8}" # 1226
                            line += f"{row['66']:6}" # 1234
                            line += f"{row['67']:1}" # 1240
                            line += f"{row['68']:13}" # 1241
                            line += f"{row['71']:20}" # 1254
                            line += f"{row['72']:25}" # 1274
                            line += f"{row['73']:50}" # 1299
                            line += f"{row['74']:50}" # 1349
                            line += f"{row['75']:10}" # 1399
                            line += f"{row['76']:10}" # 1409
                            line += f"{row['77']:10}" # 1419
                            line += f"{row['78']:1}" # 1429
                            line += f"{row['79']:30}" # 1430
                            line += f"{row['80']:15}" # 1460
                            line += f"{row['81']:255}" # 1475
                            line += f"{row['82']:10}" # 1730
                            line += f"{row['83']:2}" # 1740
                            line += f"{row['84']:5}" # 1742
                            line += f"{row['85']:1}" # 1747
                            line += f"{row['86']:255}" # 1748
                            line += f"{row['87']:50}" # 2003
                            line += f"{row['88']:2}" # 2053
                            line += f"{row['89']:20}" # 2055
                            line += f"{row['90']:1}" # 2075
                            line += f"{row['91']:255}" # 2076
                            line += f"{row['92']:255}" # 2331
                            line += f"{row['93']:50}" # 2586
                            line += f"{row['94']:1}" # 2636
                            line += f"{row['95']:3}" # 2637
                            line += f"{row['96']:10}" # 2640
                            line += f"{row['97']:20}" # 2650
                            line += f"{row['98']:1}" # 2670
                            line += f"{row['99']:6}" # 2671
                            line += f"{row['100']:6}" # 2677
                            line += f"{row['101']:5}" # 2683
                            line += f"{row['102']:5}" # 2688
                            line += f"{row['103']:5}" # 2693
                            line += f"{row['104']:5}" # 2698
                            line += f"{row['105']:5}" # 2703
                            line += f"{row['106']:5}" # 2708
                            line += f"{row['107']:10}" # 2713
                            line += f"{row['108']:8}" # 2723
                            line += f"{row['109']:5}" # 2731
                            line += f"{row['110']:6}" # 2736
                            line += f"{row['111']:30}" # 2742
                            line += f"{row['112']:6}" # 2772
                            line += f"{row['113']:6}" # 2778
                            line += f"{row['114']:6}" # 2784
                            line += f"{row['115']:3}" # 2790
                            line += f"{row['116']:3}" # 2793
                            line += f"{row['117']:3}" # 2796
                            line += f"{row['118']:10}" # 2799
                            line += f"{row['119']:4}" # 2809
                            line += f"{row['120']:2}" # 2813
                            line += f"{row['121']:6}" # 2815
                            line += f"{row['122']:1}" # 2821
                            line += f"{row['123']:1}" # 2822
                            line += f"{row['124']:1}" # 2823
                            line += f"{row['125']:1}" # 2824
                            line += f"{row['126']:10}" # 2825
                            line += f"{row['127']:10}" # 2835
                            line += f"{row['128']:10}" # 2845
                            line += f"{row['129']:20}" # 2855
                            line += f"{row['130']:50}" # 2875
                            line += f"{row['131']:5}" # 2925
                            line += f"{row['132']:60}" # 2930
                            line += f"{row['133']:60}" # 2990
                            line += f"{row['134']:60}" # 3050
                            line += f"{row['135']:30}" # 3110
                            line += f"{row['136']:25}" # 3140
                            line += f"{row['137']:15}" # 3165
                            line += f"{row['138']:1}" # 3180
                            line += f"{row['139']:25}" # 3181
                            line += f"{row['140']:15}" # 3206
                            line += f"{row['141']:1}" # 3221
                            line += f"{row['142']:75}" # 3222
                            line += f"{row['143']:70}" # 3297
                            line += f"{row['144']:25}" # 3367
                            line += f"{row['145']:2}" # 3392
                            line += f"{row['146']:5}" # 3394
                            line += f"{row['147']:4}" # 3399
                            line += f"{row['148']:10}" # 3403
                            line += f"{row['149']:70}" # 3413
                            line += f"{row['150']:25}" # 3483
                            line += f"{row['151']:2}" # 3508
                            line += f"{row['152']:5}" # 3510
                            line += f"{row['153']:4}" # 3515
                            line += f"{row['154']:10}" # 3519
                            line += f"{row['155']:8}" # 3529
                            line += f"{row['157']:1}" # 3537
                            line += f"{row['158']:2}" # 3538
                            line += f"{row['159']:2}" # 3540
                            line += f"{row['160']:3}" # 3542
                            line += f"{row['161']:8}" # 3545
                            line += f"{row['162']:8}" # 3553
                            line += f"{row['163']:30}" # 3561
                            line += f"{row['164']:25}" # 3591
                            line += f"{row['165']:25}" # 3616
                            line += f"{row['166']:4}" # 3641
                            line += f"{row['167']:25}" # 3645
                            line += f"{row['168']:15}" # 3670
                            line += f"{row['169']:1}" # 3685
                            line += f"{row['170']:25}" # 3686
                            line += f"{row['171']:15}" # 3711
                            line += f"{row['172']:1}" # 3726
                            line += f"{row['173']:9}" # 3727
                            line += f"{row['174']:60}" # 3736
                            line += f"{row['175']:40}" # 3796
                            line += f"{row['176']:2}" # 3836
                            line += f"{row['177']:5}" # 3838
                            line += f"{row['178']:4}" # 3843
                            line += f"{row['179']:1}" # 3847
                            line += f"{row['180']:80}" # 3848
                            line += f"{row['181']:25}" # 3928
                            line += f"{row['182']:2}" # 3953
                            line += f"{row['183']:5}" # 3955
                            line += f"{row['156']:10}" # 3960
                            line += f"{row['69']:30}" # 3970
                            line += f"{row['RecordId']:<10}" # 4000
                            line += f"{row['70']:1}" # 4010
                            line += '\n'
                            extr.write(line)                     
                        df=dts_adc_instance.fn_fetch(query="EXEC [dbo].[GetAdcExtract]", fetch='fetchmany', commit=False, close_conn=False,
                            get_next_buffer=True,buffer_size=buffersize, return_dataframe = True)
                    extr.close()

                #Zip CSV File
                zipfile = filename.replace(".txt",".zip")
                log.info(f'Zipping CSV file {filename} to zipfile {zipfile}')
                self.zip_util.Compress(srcefile=filename, srcefilepath="temp", destfile=zipfile, password=None, compression_level=5, deletesrce=True)

                #Copy file to SFTP
                remote_filename = os.path.join(ftp_path, Path(zipfile).name)            
                log.info(f'FTPing {zipfile} to {remote_filename}')
                sftp_conn = ftp(host=ftp_host, user=ftp_user, pw=ftp_pswd, port=ftp_port, log=log)
                sftp_conn.putfile(localpath = zipfile, remotepath = remote_filename, close_conn = True)  

                #Delete Zip from from work directory after it has been copied to SFTP
                # os.remove(zipfile)

                #Execute [dbo].[GetExtractCount] stored procedure for remaining records to process
                dts_adc_instance.fn_execute(query="EXEC [dbo].[UpdateExportTrackingBR]", commit=True, close_conn=False)

                extractcount = dts_adc_instance.fn_fetch(query="EXEC [dbo].[GetExtractCount]", fetch='fetchval', commit=False, close_conn=False)
                log.info(f'{str(extractcount)} records to process...')

        #Close DB connection
        dts_adc_instance.fn_close()

    def main(self):

        start = time.perf_counter() 
        rc = 0        

        try:
            self.Extract()
        except Exception as e:
            log.exception(e, exc_info=e)
            self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
            rc = 1
        else:
            finish = time.perf_counter() 
            log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
        finally:
            exit(rc)  

obj = ADCExtract()
obj.main()




        
