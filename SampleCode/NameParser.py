#%%
import pysftp
import datetime
import fnmatch
import pyodbc
import sys
import os
import pathlib
import pandas as pd
import time
import utils

utils.log("Root folder="+sys.path[1])

from glob import glob
from zipfile import ZipFile
from datetime import timedelta
from dynaconf import settings, Validator

# Load 2 settings files, the first being a list of settings that apply to all programs and the second a list of programs specific settings
settings.load_file(path="global_settings.toml") # .toml")

settings.validators.register(
    Validator('environment', 'debug', 'sqldriver','tempfolder', must_exist=True)
    # Validator('password', must_exist=False)
    # Validator('debug', eq='False', environment='prod')
)

# Fire the validator
settings.validators.validate()
env = str(settings.ENVIRONMENT)
utils.log("Environment="+env)

sqldriver = settings.SQLDRIVER
utils.log("sqldriver="+sqldriver)

dbhost_dts = settings.DTSDB.host
dbname_dts = settings.DTSDB.dbname
dbhost_dts = r"EDGQN1VSRCDTS04" #EDGQN1VPDSSQL28
#dbhost_dts = r"EDGQN1VPDTSQL17"
dbname_dts = r"AntiFraud"
utils.log("dbhost_dts="+dbhost_dts)
utils.log("dbname_dts="+dbname_dts)
# To accept any SFTP key
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
# Runtime
debug = False
table_srce = "dbo.TEST_LEO_NAMEVIEW"
table_dest = "dbo.TEST_LEO_NAMEVIEW"
InsertRow = False

names = []
new_names = []

def SqlConn(sqldriver, dbhost_dts, dbname_dts):
    connstring='DRIVER='+sqldriver+';SERVER='+dbhost_dts+';DATABASE='+dbname_dts+';Trusted_Connection=yes'
    utils.log("DB Connection String="+connstring)
    conn = pyodbc.connect(connstring)
    return conn

def FindCorp(corplist, parsename):
    for row in corplist.itertuples(index=False):
        if row[0] == parsename:
            return True
    return False

def BuildInsertQuery(desttable, df):
    cols = df.columns
    sqlquery = "INSERT INTO " + desttable + " ( "
    insvalues = " VALUES ( "
    for i in range(len(cols)):
        if i > 0: prfx = ", "
        else: prfx = ""
        sqlquery += prfx + "[" + str(cols[i]) + "]"
        insvalues += prfx + "?"
    sqlquery += " )" + insvalues + " )"
    return sqlquery
    
connAntiFraud = SqlConn(sqldriver, dbhost_dts, dbname_dts)
csrsAntiFraud = connAntiFraud.cursor()

query = "SET NOCOUNT ON; SELECT [Field] FROM [CorporationTags] AS [c] OUTER APPLY( SELECT * FROM [util].[dbo].[ParseDelimited]([c].[Tags], ' ') AS [b] ) AS [b] ;"
start = time.time()
utils.log("Start Read...")
corplist = pd.read_sql(query, connAntiFraud)
#print(corplist)
utils.log('End Read, Elapsed:' + str(timedelta(seconds=time.time() - start)))    

query = "SET NOCOUNT ON; SELECT [RecordId], [RawName1], [RawName2], [RawName3], [RawName4] FROM " + table_srce + " WHERE [Name1] IS NULL"
if debug: query += " WHERE REcordId in (7423469);"
start = time.time()
utils.log("Start Read...")
df = pd.read_sql(query, connAntiFraud)
num_rows = df.shape[0]
utils.log('End Read, Rows: ' + f'{num_rows:,d}' + ', Elapsed: ' + str(timedelta(seconds=time.time() - start)))    
if num_rows == 0:
    utils.log("No rows to process...")
    sys.exit(0)

# Insert Name columns to the DataFrame
df.insert(5,'Name1', None, False)
df.insert(6,'Name2', None, False)
df.insert(7,'Name3', None, False)
df.insert(8,'Name4', None, False)

def FixNames(rawname):
    tnames = []
    tnames_pb = []
    resultnames = []
    j = 0
    lastname = None
    firstname, middlename, suffixname = NameClear()
    nameparsed = rawname.split()
    for i, namepart in enumerate(nameparsed):
        #Check for Corp Tag
        findCorptag = FindCorp(corplist, namepart)
        if findCorptag:
            tnames = []
            tnames_pb = []
            tnames.append(rawname)
            lastname = None
            break
        if namepart == '&' or namepart == 'AND':
            if not (lastname is None and firstname is None and middlename is None and suffixname is None):
                # c# push_back
                tnames_pb.append(NameMerge(lastname, firstname, middlename, suffixname))
                firstname, middlename, suffixname = NameClear()
                j = 1
        elif lastname == None and j == 0:
            lastname = namepart
            j = j + 1
        elif firstname == None and j == 1:
            firstname = namepart
            j = j + 1
        elif middlename == None and len(namepart) == 1 and j == 2:
            middlename = namepart
            j = j + 1
        elif suffixname == None and j == 3:
            suffixname = namepart
            j = j + 1
        else:
            #Must be a Corporate
            tnames = []
            tnames_pb = []
            tnames.append(rawname)
            lastname = None
            break
    # End of Name loop    
    fullname = NameMerge(lastname, firstname, middlename, suffixname)
    if fullname is not None:   
        tnames.append(fullname)

    for tname in tnames_pb:
        resultnames.append(tname)

    for tname in tnames:
        resultnames.append(tname)

    return resultnames

def NameMerge(lastname, firstname, middlename, suffixname):
    fullname = None
    if lastname is not None:
        fullname = lastname
        if firstname != None and firstname > '':
            fullname += " " + firstname
            if middlename != None and middlename > '':
                fullname += " " + middlename
                if suffixname != None and suffixname > '':
                    fullname += " " + suffixname
    return fullname

def NameClear():
    firstname = None
    middlename = None
    suffixname = None
    return firstname,middlename,suffixname

startread = time.time()
utils.log("Start Processing...")
for row in df.itertuples():
    
    rawnames = []
    if row.RawName1 != None and row.RawName1 > '':
        rawnames.append(row.RawName1)
    if row.RawName2 != None and row.RawName2 > '':
        rawnames.append(row.RawName2)
    if row.RawName3 != None and row.RawName3 > '':
        rawnames.append(row.RawName3)
    if row.RawName4 != None and row.RawName4 > '':
        rawnames.append(row.RawName4)

    new_names = []
    for rawname in rawnames:        
        tnames = FixNames(rawname)
        for tname in tnames:
            new_names.append(tname)

    if len(new_names) >= 1:
        df.at[row.Index,'Name1'] = new_names[0]
    if len(new_names) >= 2:
        df.at[row.Index,'Name2'] = new_names[1]
    if len(new_names) >= 3:
        df.at[row.Index,'Name3'] = new_names[2]
    if len(new_names) >= 4:
        df.at[row.Index,'Name4'] = new_names[3]
    
    if row.Index % 50000 == 0: 
        if row.Index > 0: utils.log("\tRow Index: " + f'{row.Index:,d}' + ', Elapsed: ' + str(timedelta(seconds=time.time() - start)))
        start = time.time()

utils.log('End Processing, Elapsed: ' + str(timedelta(seconds=time.time() - startread)))

if InsertRow:
    sqlquery = BuildInsertQuery(table_dest, df)
    if debug: print(list(df.itertuples(index=False, name=None)))
    start = time.time()
    utils.log("Start Insert...")
    if not debug: csrsAntiFraud.fast_executemany = True
    if not debug: csrsAntiFraud.executemany(sqlquery, list(df.itertuples(index=False, name=None)))
    if not debug: csrsAntiFraud.commit()
    utils.log('End Insert, Elapsed: ' + str(timedelta(seconds=time.time() - start)))
else:    
    df.pop('RawName1')
    df.pop('RawName2')
    df.pop('RawName3')
    df.pop('RawName4')
    if debug: print(list(df.itertuples(index=False, name=None)))
    start = time.time()
    utils.log("Start Update...")
    if not debug: csrsAntiFraud.fast_executemany = True
    if not debug: csrsAntiFraud.executemany("EXEC [dbo].[UpdateNameView] ?, ?, ?, ?, ?", list(df.itertuples(index=False, name=None)))
    if not debug: csrsAntiFraud.commit()
    utils.log('End Update, Elapsed: ' + str(timedelta(seconds=time.time() - start)))   

