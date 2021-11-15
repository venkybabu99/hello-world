import zipfile as zip
# from pathlib import Path
import os
import pyminizip
# import pyzipper
# from configs import EnvironmentConfig
from utils import get_logger
import inspect
# import sys

# env_prop = EnvironmentConfig()

# log = get_logger('ZipUtils')

class ZipUtils(object):

    def __init__(self, log = None):
        self.log = log if log else get_logger('ZipUtils') 
    '''
    pyminizip.compress("/srcfile/path.txt", "file_path_prefix", "/distfile/path.zip", "password", int(compress_level))

    Args:
    1. src file path (string)
    2. src file prefix path (string) or None (path to prepend to file)
    3. dst file path (string)
    4. password (string) or None (to create no-password zip)
    5. compress_level(int) between 1 to 9, 1 (more fast) <---> 9 (more compress) or 0 (default)

    Return value:
    - always returns None
    '''
    def Compress(self, srcefile: str, srcefilepath: str, destfile: str, password:str = None, compression_level: int = 2, deletesrce: bool= False):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " parms...srcefile={0}, srcefilepath={1}, destfile={2}".format(srcefile,srcefilepath,destfile))
        try:
            pyminizip.compress(srcefile, srcefilepath, destfile, password, compression_level)
            if deletesrce:
                os.remove(srcefile)        
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise
        return
        
    '''
    pyminizip.compress_multiple([u'pyminizip.so', 'file2.txt'], [u'/path_for_file1', u'/path_for_file2'], "file.zip", "1233", 4, progress)
    Args:
    1. src file LIST path (list)
    2. src file LIST prefix path (list) or []
    3. dst file path (string)
    4. password (string) or None (to create no-password zip)
    5. compress_level(int) between 1 to 9, 1 (more fast) <---> 9 (more compress)
    6. optional function to be called during processing which takes one argument, the count of how many files have been compressed

    Return value:
    - always returns None
    '''
    def Compress_Multiple(self, srcefiles: list, srcefilespath: list, destfile: str, password:str = None, compression_level: int = 5, deletesrce: bool= False):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " parms...srcefiles={0}, srcefilespath={1}, destfile={2}".format(srcefiles,srcefilespath,destfile))
        try:
            pyminizip.compress_multiple(srcefiles, srcefilespath, destfile, password, compression_level)
            if deletesrce:
                for file in srcefiles:
                    os.remove(file)        
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise
        return


    def ZipInfoList(self, zipfile: str, log = None) -> list:
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " parms...zipfile={0}".format(zipfile))
        namelist = None
        try:        
            with zip.ZipFile(zipfile) as file:
                namelist = file.infolist()
                file.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise
        return namelist            


    def ZipNameList(self, zipfile: str, log = None) -> list:
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " parms...zipfile={0}".format(zipfile))
        namelist = None
        try:        
            with zip.ZipFile(zipfile) as file:
                namelist = file.namelist()
                file.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise
        return namelist            


    def ZipExtract(self, zipfile: str, filename:str, destpath: str, password: bytes = None, log = None ):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " parms...zipfile={0}, filename={1}, destpath={2}".format(zipfile,filename,destpath))
        try:        
            with zip.ZipFile(zipfile) as file:
                file.extract(member=filename, path=destpath, pwd=password)
                file.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise


    def ZipExtractAll(self, zipfile: str, destpath: str, password: bytes = None, log = None ):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " parms...zipfile={0}, destpath={1}".format(zipfile,destpath))
        try:        
            with zip.ZipFile(zipfile) as file:
                file.extractall(path=destpath, pwd=password)
                file.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise


    def ZipGetInfo(self, zipfile: str, filename:str, log = None):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " parms...zipfile={0}, filename={1}".format(zipfile,filename))
        zipinfo = None
        try:        
            with zip.ZipFile(zipfile) as file:
                zipinfo = file.getinfo(name=filename)
                file.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            # exit(1)
            raise
        return zipinfo

    # def AESZipFile(self, srcefile: str, destfile: str, password:str = None, compression_level: int = 2, deletesrce: bool= False, log = None):
    #     with pyzipper.AESZipFile(destfile, 'w', compression=pyzipper.ZIP_LZMA, encryption=pyzipper.WZ_AES) as zf:
    #         zf.setpassword(password.encode())
    #         zf.setencryption(pyzipper.WZ_AES, nbits=256)            
    #         zf.write(srcefile)
    #     if deletesrce:
    #         os.remove(srcefile)        
    #     return

