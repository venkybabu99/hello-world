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

print("Environment="+env)
tempfolder=settings.from_env(env).TEMPFOLDER
print("Temp Folder="+tempfolder)
# print(settings.from_env(env).DEBUG)
# print(settings.from_env(env).DATABASE.host) 
# print(settings.from_env(env).DATABASE.port) 
# print(settings.get('database.host','localhost'))
# dynaconf.validator.ValidationError: PASSWORD cannot exists in env test
sqldriver = settings.from_env(env).SQLDRIVER
print("sqldriver="+sqldriver)
dbhost_diablo = settings.from_env(env).DIABLODB.host
dbname_diablo = settings.from_env(env).DIABLODB.dbname
print("dbhost_diablo="+dbhost_diablo)
print("dbname_diablo="+dbname_diablo)
try:
    # Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook(tempfolder+'anton.xlsx')
    worksheet = workbook.add_worksheet()
    # Start from the first cell. Rows and columns are zero indexed.
    row = 0
    col = 0

    #Connect to DB
    connstring='DRIVER='+sqldriver+';SERVER='+dbhost_diablo+';DATABASE='+dbname_diablo+';Trusted_Connection=yes'
    print("DB Connection String="+connstring)
    cnxn = pyodbc.connect(connstring)
    cursor = cnxn.cursor()

    #Print and Output results of query to Excel
    # cursor.execute("select CntyCd, CntyName from tcommon.datasupplier")
    cursor.execute("EXEC [aTrans].[CloseCountyReportREM]")
    records=cursor.fetchall();
    for r in records:
        print(r)
        # print(r[0], r[1])
        #worksheet.write(row, col, r[0])
        #worksheet.write(row, col + 1, r[1])
        #row += 1

    cnxn.close()
    workbook.close()

except Exception as inst:
    print(type(inst))    # the exception instance
    print(inst.args)     # arguments stored in .args
    print(inst)          # __str__ allows args to be printed directly,
                         # but may be overridden in exception subclasses