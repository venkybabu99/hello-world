import datetime
import logging
import os
from datetime import datetime
from configs import AppConfig

stm = datetime.now().strftime('%H:%M:%S.%f')[:-3]


class TimeFilter(logging.Filter):
    def filter(self, record):
        global stm
        record.relative = stm
        stm = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        return True


def get_logger(mod_name):
    try:
        ''' Comment out LM 2021-09-01
        Don't see a reason why we need to create a Log folder in the current directory since we are 
        logging to the folder in the app.properties folder
        Replaced with the code in the else:, to get the log path and then check if exists
        '''
        # current_dir = os.path.join(os.getcwd())
        # create Logs dir if one does not already exist
        # if not os.path.isdir(os.path.join(current_dir, 'Logs')):
            # os.makedirs(os.path.join(current_dir, 'Logs'))
        app_con = AppConfig()
        log_path = app_con.get_log_path()
        if not os.path.isdir(log_path):
            os.makedirs(log_path)        

    except Exception as e:
        print("**Error in get_logger")
        print(e.args)
    else:
        log = logging.getLogger(mod_name)
        c_handler = logging.StreamHandler()

        # app_con = AppConfig()
        # log_path = app_con.get_log_path()
        # if not os.path.isdir(log_path):
        #     os.makedirs(log_path)        
        # log_file = log_path + "{:%m-%d-%Y}.log".format(datetime.now())
        # log_file = log_path + mod_name + "-{:%m-%d-%Y}.log".format(datetime.now())
        log_file = log_path + mod_name + "-{:%Y-%m-%d}.log".format(datetime.now())

        handler = logging.FileHandler(log_file)
        fmt = app_con.get_log_fmt()
        formatter = logging.Formatter(fmt, datefmt='%H:%M:%S')
        handler.setFormatter(formatter)
        c_handler.setFormatter(formatter)
        log.addHandler(handler)
        log.addHandler(c_handler)
        log.setLevel(logging.INFO)
        log.setLevel(logging.DEBUG)
        for handle in log.handlers:
            handle.addFilter(TimeFilter())
        return log
