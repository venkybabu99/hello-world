from configs.ConfigUtility import AppConfig
from utils import get_logger
from configs import EnvironmentConfig
from vault import vault
log = get_logger('TestVault')
import json
try:
    env_config = EnvironmentConfig()
    #Create connection to the vault
    con = vault()
    log.info("RoleName="+con.role_name)
    log.info("mount_point="+con.mount_point)
    log.info("base_path=" + con.base_path)    

    # Delete Secret
    # con.delete_secret("ADCExtract")
    # con.delete_secret("ADCLastRecording")
    # con.delete_secret("AntiFraud")
    # con.delete_secret("DQImport")
    # con.delete_secret("DQMCExtract")
    # con.delete_secret("DQMMExtract")
    # con.delete_secret("DocumentCountsExport")

    # Create or update secret 
    # log.info("hier")    
    # x =  '{"user":"dtsdev", "password":"OS5rdUsD"}'
    # x = '{"pid7":[{"ftp_user":"itgadmin"},{"ftp_pswd":"d2rHdBPc&AsY"}], "pid5":[{"ftp_user":"res_lasn"},{"ftp_pswd":"f4r5sd1"}], "pid2":[{"ftp_user":"itgadmin"},{"ftp_pswd":"d2rHdBPc&AsY"}], "pid13":[{"ftp_user":"itgadmin"},{"ftp_pswd":"d2rHdBPc&AsY"}], "pid9":[{"ftp_user":"itgadmin"},{"ftp_pswd":"d2rHdBPc&AsY"}], "pid90":[{"ftp_user":"c0r3l0g!c"},{"ftp_pswd":"r!v3rs!d3"}], "extract":[{"ftp_user":"dtsdev"},{"ftp_pswd":"OS5rdUsD"}]}'
    # con.update_secret("AntiFraud", x)    

    # secret = '{"password":"OS5rdUsD"}'
    # con.update_secret(path="dtsdev", secret= secret)
    # secret = '{"password":"d2rHdBPc&AsY"}'
    # con.update_secret(path="itgadmin", secret= secret)
    # secret = '{"password":"VCap$76"}'
    # con.update_secret(path="V2Capture", secret= secret)
    # secret = '{"password":"f4r5sd1"}'
    # con.update_secret(path="res_lasn", secret= secret)
    # secret = '{"password":"r!v3rs!d3"}'
    # con.update_secret(path="c0r3l0g!c", secret= secret)
    # secret = '{"password":"FRS724"}'
    # con.update_secret(path="dqadmin", secret= secret)
    # secret = '{"password":"asgs_one"}'
    # con.update_secret(path="sacuser1", secret= secret)

    # x =  '{"user":"dtsdev", "password":"OS5rdUsD"}'
    # con.update_secret("ADCExtract", x)    
    # log.info("After update_secret")

    # # Get and print secret
    # print("**Get and print Secret**")
    # read_response = con.get_secret("c0r3l0g!c")
    # print("DQImport")
    # print(read_response)
    # read_response = con.get_secret("dtsdev")    
    # print("ADCExtract")
    # print(read_response)    

    # print("**List Secrets**")
    read_response = con.list_secrets(con.base_path)
    # print("**List Secrets Hier**")    
    # log.info("After list_secrets")    
    # print(read_response)    
    if not read_response is None:
        print(read_response['data']['keys'])
except Exception as e:
    log.error(e)
    exit