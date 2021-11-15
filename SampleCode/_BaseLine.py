# Initial Author: ???
# Date Written: ????
# Overview: ** Describe at a high level what this program is doing
# 
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
# 
###########################################################################################
import pyodbc

from dynaconf import settings, Validator
# Load 2 settings files, the first being a list of settings that apply to all programs and the second a list of programs specific settings
settings.load_file(path="/global.toml;/_baseline.toml")

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
sqldriver = settings.from_env(env).SQLDRIVER
dbhost_diablo = settings.from_env(env).DIABLODB.host
dbname_diablo = settings.from_env(env).DIABLODB.dbname
print("sqldriver="+sqldriver)
print("dbhost_diablo="+dbhost_diablo)
print("dbname_diablo="+dbname_diablo)
try:
    #Connect to DB
    connstring='DRIVER='+sqldriver+';SERVER='+dbhost_diablo+';DATABASE='+dbname_diablo+';Trusted_Connection=yes'
    print("DB Connection String="+connstring)
    cnxn = pyodbc.connect(connstring)
    cursor = cnxn.cursor()

    cursor.execute("select CntyCd, CntyName from tcommon.datasupplier")
    records=cursor.fetchall();
    for r in records:
        print(r)

    cnxn.close()
  
except Exception as inst:
    print("*** Error Information ***")
    print(type(inst))    # the exception instance
    print(inst.args)     # arguments stored in .args
    print(inst)          # __str__ allows args to be printed directly,
                         # but may be overridden in exception subclasses

# Finally - any code that needs to execute regardless of whether the execution was successful or not goes here