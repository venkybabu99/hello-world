import configparser
import os
import ast
from re import escape 

class ConfigUtility(object):
    __slots__ = ['_config', 'root_dir','runtime_environment']

    def __init__(self):
        self._config = configparser.RawConfigParser()
        self.root_dir = os.path.dirname(os.path.abspath(__file__))
        # Runtime environment mist be dev, uat or prod.  This will determine which configuration files are used
        self.runtime_environment = os.getenv('DTS_Environment','dev')  # Default to dev if environment variable not set

class EnvironmentConfig(ConfigUtility):
    __slots__ = ['config_file_path']

    def __init__(self):
        super().__init__()
        self.config_file_path = os.path.join(self.root_dir, "environment.properties." + self.runtime_environment)
        self._config.read(self.config_file_path)

    def local_path(self):
        return self._config.get("DATA", "local_path")

    '''DIABLO and EDG DB SQL SERVER CONNECTION'''

    def get_edg_connection(self):
        return self._config.get("SQLSERVER", "conn_type")

    # def get_odbc_driver(self):
    #     return self._config.get("SQLSERVER", "odbc_driver")

    def get_sql_driver(self):
        return self._config.get("SQLSERVER", "sql_driver")

    def get_diablo_server(self):
        return self._config.get("Diablo", "diablo_server")

    def get_diablo_db(self):
        return self._config.get("Diablo", "diablo_db_name")

    def get_diablo_fulfillment_server(self):
        return self._config.get("Diablo", "fulfillment_server")

    def get_diablosynonyms_db(self):
        return self._config.get("Diablo", "diablosynonyms_db_name")

    def get_dts_server(self):
        return self._config.get("DTS", "dts_server")

    def get_develop_dts_server(self):
        return self._config.get("DTS", "develop_dts_server")

    def get_dts_autoreports_db(self):
        return self._config.get("DTS", "autoreports_db_name")

    def get_dts_pvcstracker_db(self):
        return self._config.get("DTS", "pvcstracker_db_name")

    def get_dts_dqweb_db(self):
        return self._config.get("DTS", "dqweb_db_name")

    def get_dts_dataquick_db(self):
        return self._config.get("DTS", "dataquick_db_name")

    def get_dts_readme_db(self):
        return self._config.get("DTS", "readme_db_name")

    def get_dts_import_db(self):
        return self._config.get("DTS", "import_db_name")

    def get_dts_antifraud_db(self):
        return self._config.get("DTS", "antifraud_db_name")
    
    def get_dts_inventorytracker2_db(self):
        return self._config.get("DTS", "inventorytracker2_db_name")
    
    def get_dts_countyapnprofile_db(self):
        return self._config.get("DTS", "countyapnprofile_db_name")
    
    def get_dts_documentcounts_db(self):
        return self._config.get("DTS", "documentcounts_db_name")
        
    def get_dts_eagle_db(self):
        return self._config.get("DTS", "eagle_db_name")

    def get_dts_qcpro_db(self):
        return self._config.get("DTS", "qcpro_db_name")
    
    def get_dts_adc_db(self):
        return self._config.get("DTS", "adc_db_name")

    def get_dts_global_db(self):
        return self._config.get("DTS", "global_db_name")

    '''END ---- DIABLO and EDG DB SQL SERVER CONNECTION'''

    def get_from_email(self):
        return self._config.get("EMAIL", "from_email")

    # def get_email_from_pwd(self):
    #     return self._config.get("EMAIL", "from_pwd")

    def get_email_to_email(self):
        return self._config.get("EMAIL", "to_email")

    def get_exception_to_email(self):
        return self._config.get("EMAIL", "to_email_exception")

    # def get_email_subject(self):
    #     return self._config.get("EMAIL", "email_subject")

    def get_vault_role(self):
        return self._config.get("VAULT", "role_name")

    def get_vault_mount_point(self):
        return self._config.get("VAULT", "mount_point")

    def get_vault_namespace(self):
        return self._config.get("VAULT", "vault_namespace")

    def get_vault_base_path(self):
        return self._config.get("VAULT", "base_path")

    def get_addressplusbatch(self, parm: str):
        return self._config.get("AddressPlusBatch", parm)

class AppConfig(ConfigUtility):
    __slots__ = ['config_file_path']

    def __init__(self):
        super().__init__()
        self.config_file_path = os.path.join(self.root_dir, "app.properties." +self.runtime_environment)
        self._config.read(self.config_file_path)

    def get_parm_value(self, section: str, parm: str):
        try:
            value = self._config.get(section, parm)
        except:
            value = None
        return value
  

    ''' LOG  CONFIGURATIONS'''

    def get_log_path(self):
        return self._config.get("LOGS", "log_path")

    def get_log_fmt(self):
        return self._config.get("LOGS", "log_fmt")