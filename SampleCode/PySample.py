###########################################################################################
# Author: Anton
# Date: April 2021
# Overview: Test Python
# - Importing Configurations
# - Connect to SQL Server
# - Error Handling
# - Create Excel
###########################################################################################

import pyodbc
import xlsxwriter
import zipfile
import gzip
import shutil
import csv
import pysftp

from os.path import basename
from base64 import decodebytes
from dynaconf import settings, Validator
settings.load_file(path="/settings.toml")

settings.validators.register(
    Validator('environment', 'debug', must_exist=True)
    # Validator('password', must_exist=False)
    # Validator('debug', eq='False', environment='prod')
)

# Fire the validator
settings.validators.validate()
env = str(settings.ENVIRONMENT)

print("Environment"+env)
tempfolder=settings.from_env(env).TEMPFOLDER
print("Temp Folder="+tempfolder)
# print(settings.from_env(env).DEBUG)
# print(settings.from_env(env).DATABASE.host) 
# print(settings.from_env(env).DATABASE.port) 
# print(settings.get('database.host','localhost'))
# dynaconf.validator.ValidationError: PASSWORD cannot exists in env test

dbhost = settings.from_env(env).DATABASE.host
dbname = settings.from_env(env).DATABASE.dbname
print("dbhost="+dbhost)
print("dbname="+dbname)
# connection = pyodbc.connect(server=dbhost, database=dbname)
# connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+dbhost+';DATABASE='+dbname)

try:
    # Create a workbook and add a worksheet.
    excelfile = tempfolder+'anton.xlsx'
    workbook = xlsxwriter.Workbook(excelfile)
    worksheet = workbook.add_worksheet()
    # Start from the first cell. Rows and columns are zero indexed.
    row = 0
    col = 0
    header = {'CntyCd', 'CntyName'};
    worksheet.write(row, col, 'CntyCd')
    worksheet.write(row, col + 1, 'CntyName')
    row += 1

    #Connect to DN
    connstring='DRIVER={SQL Server Native Client 11.0};SERVER='+dbhost+';DATABASE='+dbname+';Trusted_Connection=yes'
    print("DB Connection String="+connstring)
    conn = pyodbc.connect(connstring)
    cursor = conn.cursor()

    #Print and Output results of query to Excel
    cursor.execute("select CntyCd, CntyName from tcommon.datasupplier")
    records = cursor.fetchall()

    for record in records:
        print(record[0], record[1])
        worksheet.write(row, col, record[0])
        worksheet.write(row, col + 1, record[1])
        row += 1

    with open(r'C:\Temp\_Python\Demo.csv','w',newline = '') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])
        for record in records:
            writer.writerow(record)

    workbook.close()

# Zip File
    with zipfile.ZipFile(excelfile+".zip", mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=2, allowZip64=True) as excelzipped:
        excelzipped.write(excelfile,basename(excelfile))
    excelzipped.close()

# GZip File
    with open(excelfile, 'rb') as excelin:
        with gzip.open(excelfile+".gz", 'wb') as excelgzip:
            shutil.copyfileobj(excelin, excelgzip)
    excelgzip.close()
    excelin.close()

# Sftp File
# Accept any host key (still wrong see below)
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
# And authenticate with a private key
    with pysftp.Connection(host="ftp2.resftp.com", username="Leads2Loans", password="L2loans", log="c:/temp/pysftp.log", private_key=".ppk", cnopts=cnopts) as sftp:
       sftp.cwd('/demographics_/_LM')  # The full path
       sftp.put(excelfile)  # Upload the file
       sftp.put(excelfile+".zip")  # Upload the file
       sftp.put(excelfile+".gz")  # Upload the file

except Exception as inst:
    print(type(inst))    # the exception instance
    print(inst.args)     # arguments stored in .args
    print(inst)          # __str__ allows args to be printed directly,
                         # but may be overridden in exception subclasses
conn.close()
