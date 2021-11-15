# Initial Author: Anton
# Date Written: May 2021
# Overview: This file contaons reusable modules that can be used in other executables
#
#           Note:  I have no try/catch in here.  I am assuming that the calling application will catch errors
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
###########################################################################################
import os
import smtplib
from email.message import EmailMessage
import inspect
from utils import get_logger
from configs import EnvironmentConfig
import mimetypes
from datetime import datetime
# import ast

env_prop = EnvironmentConfig()

# log = get_logger('EmailUtil')

class EmailUtil(object):

    def __init__(self, log = None):
        self.log = log if log else get_logger('EmailUtil') 

    def sendemail(self,
            ToAddress: str,
            Subject: str,
            CCAddress: str="ascheepers@corelogic.com",
            FromAddress: str="EDGDTSRancho.DARS@corelogic.com",
            BCCAddress: str="",
            Body="",
            attachments: list=None
            ):

        ToAddress = ToAddress.replace(';',',')
        if CCAddress:
            CCAddress = CCAddress.replace(';',',')
        if BCCAddress:
            BCCAddress = BCCAddress.replace(';',',')

        self.log.info(f'fnc: {inspect.getframeinfo(inspect.currentframe()).function}, Parameters: FromAddress={FromAddress}, ToAddress={ToAddress}, Subject={Subject}, Attachments={attachments}')

        try:        
            msg = EmailMessage()
            msg['From'] = FromAddress
            msg['To'] = ToAddress
            if CCAddress:
                msg['CC'] = CCAddress
            if BCCAddress:
                msg['BCC'] = BCCAddress
            msg['Subject'] = Subject
            if Body:
                msg.set_content(Body)

            # Add attachments
            for attachment in attachments or []:
                ctype, encoding = mimetypes.guess_type(attachment)       
                if ctype is None or encoding is not None:
                    ctype = 'application/octet-stream'                 
                maintype, subtype = ctype.split('/', 1)                
                with open(attachment, 'rb') as fp:
                    msg.add_attachment(fp.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(attachment))                
        
            self.log.info("Sending email")
            server = smtplib.SMTP('smtp.corelogic.com: 25')
            server.send_message(msg)
            server.quit()
            self.log.info("Email sent successfully")
        except Exception as e:
            self.log.exception(e, exc_info=e)    
            raise


    def send_failure(self, app_name, error_message: str, filename: list):

        try:
            body=f'\nApp:\t{app_name}\n\nError:\t{error_message}'
            From_Email = env_prop.get_from_email()
            To_Address = env_prop.get_exception_to_email()
            self.sendemail(FromAddress=From_Email,ToAddress=To_Address,Subject=f'{app_name} Caught Exception ' + datetime.now().strftime('%Y-%m-%d %X'), attachments=filename, Body=body, CCAddress=None)     
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise

