#########################################################################################################
# GCE Authentication method to Vault                                                                    #
# See https://readthedocs.org/projects/hvac/downloads/pdf/stable/ for hvac documentation                #
# See https://confluence.corelogic.net/pages/viewpage.action?pageId=225348677 for Namespace information #
#
# Vault location for secrets:
#  {environment}/{organization}/secret/{ecosystem}/{service}
# Example:
#  nonprd/edg_legacy_us/secret/datamssql/[app, iac or data]/....
#  prd/edg_legacy_us/secret/datamssql/[app, iac or data]/....
#
# Since the nonprd link does not distinguish between the different environments, e.g. dev and uat, 
# we will need to make provision for that via "folders" within vault, e.g.
#  nonprd/edg_legacy_us/secret/datamssql/[app, iac or data]/dev/{service}
#  nonprd/edg_legacy_us/secret/datamssql/[app, iac or data]/uat/{service}
#
# For our purposes, we are going to define a new variable named 'basepath', which will be unique for each 
# environment.  That basepath will consist of {ecosystem}/[app, iac or data]/[dev, uat], i.e. the 
# path to all the secrets available for the environment we are executting in.  Since production
# has only a single environment, the will be no environment identifier needed in prod.  Therefore,
# base paths could look like follows:
#   dev: datamssql/app/dev
#   uat: datamssql/app/uat
#   production: datamssql/app/
#
# Since the vault class will be setup with the base path as parm, any subsequent calls will only need
# to identify the specific secret it needs to work with.
#########################################################################################################
import hvac
import json
import requests
from configs import EnvironmentConfig
# from configs.ConfigUtility import AppConfig
from utils import get_logger
import inspect

# log = get_logger('vault')

env_obj = EnvironmentConfig()

class vault(object):

    def __init__(self, log = None):
        self.log = log if log else get_logger('vault')
        # self.app_obj = AppConfig()
        # Conect to vault
        self.role_name = env_obj.get_vault_role()
        self.mount_point = env_obj.get_vault_mount_point()
        self.vault_namespace  = env_obj.get_vault_namespace()
        self.base_path = env_obj.get_vault_base_path()
        self.log.info("role_name="+self.role_name+" ,mount_point="+self.mount_point+" ,vault_namespace="+self.vault_namespace)   
        vault_url="https://vault-prd.solutions.corelogic.com:8200"
        self.con = None
        try:
            response = requests.get(url='http://metadata/computeMetadata/v1/instance/service-accounts/default/identity', 
                headers = {"Metadata-Flavor":"Google"}, 
                params = {"audience":"http://vault/" + self.role_name, "format":"full"})
            # init client WITHOUT namespace to auth in the vault using gce auth method
            client = hvac.Client(url=vault_url)

            # login into the vault
            client.auth.gcp.login(
            mount_point=self.mount_point,
            role=self.role_name,
            jwt=response.text,
            )

            # init new client WITH namespace using the token from gce authentication
            self.con = hvac.Client(url=vault_url, token=client.token, namespace=self.vault_namespace)
        except Exception as e:
            self.log.error("Error connecting to Vault")
            self.log.exception(e, exc_info=e)
            raise

        if self.con is None:
            self.log.error('No connection to {} {}'.format(self.role_name, self.vault_namespace))
            raise

    def delete_secret_version(self, path,versions:dict = None):  
    # Delete 1 or more versions of a secret
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", path=" + path) #  + " , versions=" + versions)        
        try:
            if versions is None: #Delete the latest version only
                self.con.secrets.kv.v2.delete_latest_version_of_secret(path=self.base_path + path,)
            else: #Delete 1 or more versions as specified by parameter
                self.con.secrets.kv.v2.delete_latest_version_of_secret(path=self.base_path + path,versions=versions)
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise

    def delete_secret(self, path):  
    # Delete secret and all metadata associated with the secret
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", path=" + path)        
        try:
            self.con.secrets.kv.v2.delete_metadata_and_all_versions(path=self.base_path + path,)
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise

    def get_secret(self, path):  
    # Return a single secret
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", path=" + path)            
        try:
            fullpath = self.base_path + path
            self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", fullpath=" + fullpath)        
            return(self.con.secrets.kv.read_secret_version(path=fullpath)['data']['data'])
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise

    def get_secret_metadata(self, path):  
    # Return metadata associated with a secret
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", path=" + path)        
        try: 
            return(self.con.secrets.kv.v2.read_secret_metadata(path=self.base_path + path,))
            #print('Secret under path hvac is on version {cur_ver}, with an oldest version of {old_
            #˓→ver}'.format(
            #cur_ver=hvac_path_metadata['data']['oldest_version'],
            #old_ver=hvac_path_metadata['data']['current_version'],
            #))
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise

    def list_secrets(self, path):  
    # Return a list of secrets from a specific path
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", path=" + path)        
        try:
            return(self.con.secrets.kv.v2.list_secrets(path=path))
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise

    def update_secret(self, path, secret: json):  
    # Create/Update 1 or more secrets
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", path=" + path + " , secret=" + secret)        
        try:
            y = json.loads(secret)
            self.con.secrets.kv.v2.create_or_update_secret(path=self.base_path + path, secret=y, ) # dict(pssst='this is secret'))
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise