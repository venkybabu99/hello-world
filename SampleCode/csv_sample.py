
import pyodbc
import csv

conn= pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=EDGQN1VDSQLKP01;DATABASE=WebHarvesting_Taxroll;trusted_connection=yes;UID=;PWD=')
cursor = conn.cursor()


try:
    cursor.execute("select top 10 * from TAX_AZ_Maricopa")
    records = cursor.fetchall()
    for record in records:
        print (record)


    with open(r'C:\Users\dskandrani\Desktop\Python_SQL_Demo\Demo.csv','w',newline = '') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])
        for record in records:
            writer.writerow(record)

except Exception as SQLError:
    print (SQLError)

conn.close()