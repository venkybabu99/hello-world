import pandas as pd
import requests 
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
import numpy as np
import time
import json
from sys import exit

def log(msg):
############ # 
    print(time.strftime("%H:%M:%S", time.localtime())+" - "+str(msg))

def QtrToDate(val):
###################  
    return val.replace("-Q1","-01-01").replace("-Q2","-04-01").replace("-Q3","-07-01").replace("-Q4","-09-01")

def removeOneTag(text, tag):
    return text[:text.find("<"+tag+">")] + text[text.find("</"+tag+">") + len(tag)+3:]

#df = pd.DataFrame(["Q1","1990-Q1"])
#df.head()
#df.applymap(QtrToDate, na_action='ignore')
#QtrToDate("Q1")
#QtrToDate("1990-Q1")
#input("Press any key to continue")
#engine = create_engine("mssql+pyodbc://@fulfillment-diablo-dev.infosolco.com/IHS_Global?driver=ODBC+Driver+17+for+SQL+Server")
#connection = engine.raw_connection()

# List of query ids that are used to extract specified data from the website, tied to the name given to the output
qrylist = pd.DataFrame(
    np.array([
        #[587304,"IHS_CBSA_GMP_1"],
        #[587306,"IHS_CBSA_GMP_2"],
        [570444,"IHS_CBSA_POP_DPI_1"]
        #[558607,"IHS_CBSA_POP_DPI_2"], 
        #[558604,"IHS_CBSA_UR_HS_1"],  
        #[558605,"IHS_CBSA_UR_HS_2"]
        #[587833,"IHS_States_DPI_POP_GSP_UR_HS_1"], 
        #[587856,"IHS_States_DPI_POP_GSP_UR_HS_2"],
        #[558612,"IHS_US_A_1"],
        #[558619,"IHS_US_M_1a"],
        #[558620,"IHS_US_Q_f"],
        #[558611,"IHS_US_Q_1"],
        #[558621,"IHS_US_Q_2"]
        ]),columns=['qry','tbl'])
try:
    for i in range(len(qrylist)):
        qry = qrylist.loc[i, 'qry']
        tbl = qrylist.loc[i, 'tbl']
        
        log("Downloading result for dataset: "+tbl)
        webURL= "https://connect.ihsmarkit.com/ExportToExcel/ExportWorkbookSeriesToExcel?workfileId="+qry+"&splitType=NoSplit&isVericalDates=False&leftPadding=1&topPadding=6&numberOfDecimals=15&rowsBetweenFrequencies=0&serializedSortOptions=%5B%7B%22SortDirection%22%3A%22asc%22%2C%22SortKey%22%3A%22Concept%22%2C%22IsOnlySortItem%22%3Atrue%7D%5D&isGroupingEnabled=False&includeWeightedIndexPartials=True&exportDateMode=StartOfPeriod&frequenciesOrder=LowToHigh&excelFormat=Html"

        #webURL= "https://connect.ihsmarkit.com/ExportToExcel/ExportWorkbookSeriesToCSV?workfileId="+qry # +"&Style=Plain&DecimalsPlaces=6" # Html"
        result = requests.get(webURL,auth=("onesourcenotify.sac.ca@corelogic.com", "B1yc3292$"))
        if result.status_code != requests.codes.ok:
            log("Error from https://connect.ihsmarkit.com is "+result.status_code)
            exit(1)
        # print(result.text)
        #t = removeOneTag(result.text,"script")
        #t = removeOneTag(t,"script")
        print("Headers="+result.headers['content-type'])
        print("Encoding="+result.encoding)
        j = json.loads(result.text)
        print(j)
        input("press enter to continue")
        print(result.content)
        input("press enter to continue")
        f = open("D:\\temp\\_anton\\result.xls", "w")
        f.write(result.text)
        f.close()
        #print(xmltojson.parse(t))        
        # print(xmltodict.parse(t))
        #print(result.text)
        input("press enter to continue")
        #log("result.text'"+result.text)
        #input("Press any key to continue")
        #log("Parsing html for dataset: "+ds)
        # Parse first table in html.  We expect only 1 table returned by each query
        # print(result.text,  file=open(r'D:\temp\_anton\result.txt', 'w'))
        df = pd.read_html(result.text)[0].applymap(QtrToDate,na_action='ignore').drop([0,1,2,3,4])
        # Update Column Names
        colNames = ['Concept','Mnemonic','LongLabel','Geography']
        i=4
        while i < len(df.columns+1):
            colNames.append('F' + str(i+1))
            i=i+1
        df.columns = colNames
        while i < 255:
            df.insert(i, 'F' + str(i+1), '0', allow_duplicates = False)
            i=i+1
        print(df.loc[df['Geography'] == 'Abilene, TX'])
        input("Enter")
        #i=5
        # Set column types for columns F5 to F255 to float
        #while i < 256:
        #    dfdetail = dfdetail.astype({'F' + str(i): 'float'})
        #    i=i+1
        #log("Updated column names="+df.columns)

        # Update the embedded header row, changing quarter markers to starting dates, e.g. 1990-Q1 to 1990-01-01
        #df.applymap(QtrToDate,na_action='ignore')
        #print(df.head(6))
        #input("Press any key to continue")
        #print(df.info())
        #df.convert_dtypes()
        # df1=df.applymap(QtrToDate,na_action='ignore').drop([0,1,2,3,4,5])
        #print(df.head(6))
        #print(df.dtypes)

        df.to_sql(tbl, engine, schema='nro', if_exists='replace', index=False, index_label=None, chunksize=200, dtype=None, method=None)
        # ExecuteStoredProc(connection,sp)
    # Execute SP to apply updates from temp tables to actual tables
    #log("Executing SP to update tables")
    #cursor = conn.cursor()
    #cursor.execute("execute nro.UpdateTables")
    #cursor.close()
    #connection.commit()

except Exception as inst:
    log("*** Error  encountered ***")
    log(type(inst))    # the exception instance
    log(inst.args)     # arguments stored in .args
    log(inst)          # __str__ allows args to be printed directly,
                         # but may be overridden 
finally:
    connection.close()
