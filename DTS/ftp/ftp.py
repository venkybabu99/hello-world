import pysftp
# from configs import EnvironmentConfig
from utils import get_logger
import inspect

# log = get_logger('ftp')

# env_obj = EnvironmentConfig()

class ftp(object):
    def __init__(self, host, user, pw=None, port: int = 22, remotefolder: str = "/", key_file=None, private_key=None,host_key_check: bool = False, compress: bool = False, log=None):
        # Returns an SFTP connection object  
        self.log = log if log else get_logger('ftp')
        self.log.info("host="+host+" ,port="+str(port)+" ,user="+user +" remotefolder="+remotefolder)   
        self.con = None
        try:
            cnopts = pysftp.CnOpts()
            if not host_key_check:
                cnopts.hostkeys = None
            cnopts.compression = compress
            conn_params = {
                'host': host,
                'port': port,
                'username': user.strip(),
                'cnopts': cnopts
            }
            if pw:
                conn_params['password'] = pw.strip()
            if key_file:
                conn_params['private_key'] = key_file
            if private_key:
                conn_params['private_key_pass'] = private_key
            self.con = pysftp.Connection(**conn_params)
            self.con.chdir(remotefolder)
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

        if self.con is None:
            self.log.error('No connection to {} {}'.format(host, user))
            # exit(1)
            raise ('No connection to {} {}'.format(host, user))

    def changedir(self, remotefolder: str = "/"):  
    # Change remote directory
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotefolder=" + remotefolder)        
        try:
            self.con.chdir(remotefolder)
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def close(self):  
    # Close connection
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))        
        try:
            self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def execute(self, cmd: str = None):  
    # Executes a command on the remote server
    # Returns:	(list of str) representing the results of the command
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " ,command=" + cmd)    
        result=None
        try:
            if not cmd == None:
                result = self.con.execute(cmd)
            return result
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def getfile(self, remotepath, localpath: str = "c:/temp/ftp.txt", close_conn: bool = False):    
    # Copy 1 file from a remote folder to a local folder
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotepath=" + remotepath + ", localpath=" + localpath)
        try:
            self.con.get(remotepath, localpath=localpath, preserve_mtime=True)
            if close_conn:
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def getfolder(self, remotefolder, localfolder: str = "c:/temp/anton/", close_conn: bool = False):    
    # Copy all files from a remote folder to a local folder
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotefolder=" + remotefolder + ", localfolder=" + localfolder)
        try:
            self.con.get_d(remotefolder, localdir = localfolder, preserve_mtime=True)
            if close_conn:
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def listdir(self, remotefolder: str = "/", close_conn: bool = False):
    # Return a directory listing
    # Returns:	(list of str) directory entries, sorted
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotefolder=" + remotefolder)
        l=None
        try:        
            l = self.con.listdir(remotefolder)
            if close_conn:
                self.con.close()
            return l
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def listdir_attr(self, remotefolder: str = "/", close_conn: bool = False):
    # Return a directory listing
    # Returns:	(list of str) directory entries, sorted
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotefolder=" + remotefolder)
        l=None
        try:        
            l = self.con.listdir_attr(remotefolder)
            if close_conn:
                self.con.close()
            return l
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise
            
    def putfile(self, localpath, remotepath = None, close_conn: bool = False):    
    # Copy 1 local file to the remote server
    # Returns:	(obj) SFTPAttributes containing attributes about the given file
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotepath=" + remotepath + ", localpath=" + localpath)
        attr = None
        try:
            attr = self.con.put(localpath, remotepath, preserve_mtime=True)
            if close_conn:
                self.con.close()
            return attr
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def putfolder(self, localpath, remotepath, recursive: bool = False, close_conn: bool = False):    
    # Copy al files in a local folder to the remote server.  If recursive is set to true, all subfolders are also copied
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotepath=" + remotepath + ", localpath=" + localpath)
        try:
            if recursive:
                self.con.put_r(localpath, remotepath, preserve_mtime=True)
            else:
                self.con.put_d(localpath, remotepath, preserve_mtime=True)
            if close_conn:
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def removefile(self, remotepath, close_conn: bool = False):    
    # Remove a file from the remote server
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotepath=" + remotepath)
        try:
            self.con.remove(remotepath)
            if close_conn:
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def removefolder(self, remotepath, close_conn: bool = False):    
    # Remove a folder from the remote server
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotepath=" + remotepath)
        try:
            self.con.rmdir(remotepath)
            if close_conn:
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def rename(self, remotesrc, remotetrg, close_conn: bool = False):    
    # Rename a file or folder on the remote server
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", remotesrc=" + remotesrc + ", remotetrg=" + remotetrg)
        try:
            ''' Check if the destination file already exists and delete it '''
            if self.con.exists(remotetrg):
                self.removefile(remotepath=remotetrg, close_conn=False)
            self.con.rename(remotesrc, remotetrg)
            if close_conn:
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise

    def is_sftp_conn_active(self) -> bool:
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        active_conn = True
        try:        
            # self.con.execute('pwd')
            _ = self.con.pwd
        except Exception as e:
            self.log.info(e)
            active_conn = False
            pass
        return active_conn
