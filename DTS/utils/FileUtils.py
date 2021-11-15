# from configs.ConfigUtility import AppConfig
from utils import get_logger
# from configs import EnvironmentConfig
from pathlib import Path
import os
import csv
from datetime import timedelta

# env_prop = EnvironmentConfig()

# log = get_logger('FileUtils')

class FileUtils(object):

    def __init__(self, log = None):
        self.log = log if log else get_logger('FileUtils') 


    def CheckDirExists(self, folder: str, createdir: bool = True) -> bool:
        path = Path(folder)
        direxists = True
        if not path.exists():
            if createdir:
                try:
                    path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    self.log.exception(e, exc_info=e)
                    raise
            else:
                direxists = False
        return direxists


    def RemoveDir(self, folder: Path, forceremove: bool = False):
        if not is_folder_empty(folder) and not forceremove:
            return False
        try:
            if isinstance(folder,str): folder = Path(folder)
            if not folder.is_dir(): return
            for p in reversed(list(folder.rglob("*"))):
                if p.is_file(): p.unlink()
                elif p.is_dir(): p.rmdir()
            folder.rmdir()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise
        return True


    def RemoveFile(self, file: str):
        try:
            if os.path.isfile(file):
                os.remove(file)
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise
        return


    def WriteCSVfile(self, File: str, Output):
        fp = open(File, 'w', newline='')
        csvfile = csv.writer(fp)
        csvfile.writerows(Output)
        fp.close()

    def seconds_to_hhmmss(self, seconds: float)->str:
        return f'{str(timedelta(seconds=round(seconds))):0>8}'

    
    # Function to remove first and last slash
    def RemovePrefixSufixSlash(self, slash:str = '/', path:str = None)->str:
        path_list = path.split(slash)
        path = None
        for p in path_list:
            if p:
                path = f'{path}{slash}{p}' if path else f'{p}'
        return path


def is_folder_empty(folder):
    return False if any(os.scandir(folder)) else True

