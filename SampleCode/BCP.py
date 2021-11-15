# Author: Anton
# Date: April 2021
# Overview: Test Python
# - Importing Configurations
# - Connect to SQL Server
# - Error Handling
# - BCP
###########################################################################################

import bcp

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
dbhost_diablo = settings.from_env(env).DIABLODB.host
dbname_diablo = settings.from_env(env).DIABLODB.dbname
print("dbhost_diablo="+dbhost_diablo)
print("dbname_diablo="+dbname_diablo)
try:
    #Connect to DB
    #connstring='driver=mssql;host='+dbhost_diablo
    #print("DB Connection String="+connstring)
    conn = bcp.Connection(driver='mssql',host=dbhost_diablo)
    my_bcp = bcp.BCP(conn)
    outfile=tempfolder+'anton.csv'
    print("outfile="+outfile)
    # file = bcp.DataFile(file_path=outfile, delimiter=',')
    file = bcp.DataFile(file_path='d:/temp/_anton/anton.csv', delimiter=',')
    # errfile=bcp.files.ErrorFile('d:\temp\_anton\anton.err')
    # my_bcp.dump(query='EXEC [aTrans].[CloseCountyReportREM]', output_file=file)
    # print("hier")
    my_bcp.dump(query='select * from Diablosynonyms.tcommon.datsupplier', output_file=file)
    # print("hier1")
    conn.close()

except Exception as inst:
    print ("hier")
    print(type(inst))    # the exception instance
    print(inst.args)     # arguments stored in .args
    print(inst)          # __str__ allows args to be printed directly,
                         # but may be overridden in exception subclasses