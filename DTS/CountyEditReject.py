# Initial Author: Anton
# Date Written: May 2021
# Overview: 1. Rewrite CountyEditReject Report SSIS Package to Python
#           2. This process
#               a. Pulls data from Diablo into 2 tables in the pvcstracker
#                  database
#               b. formats the data into an Excel spreadsheet
#               c. Sends an email with the excel spreadsheet attached
#
# History: To be completed for each change made after initial release
# Who:      When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=pvcstracker	I/O
# SQL Server=live-diablo-dev.infosolco.com
# SQL DB=Diablo
##############################################################################
import os
import time
from concurrent.futures import ThreadPoolExecutor
import inspect
#
from configs.ConfigUtility import AppConfig
from configs import EnvironmentConfig
from database import Database
from utils import get_logger
from utils.EmailUtil import EmailUtil
from utils.FileUtils import FileUtils
# ###################################

log = get_logger('CountyEditReject')

class CountyEditReject:
	
	def __init__(self):
		self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
		log.info(f'{self.app_name} object created...')
		self.env_config = EnvironmentConfig()
		self.app_config = AppConfig()
		self.email_util = EmailUtil(log=log)
		self.file_util = FileUtils(log=log)
	
	def copy_edits(self):
		
		func = inspect.getframeinfo(inspect.currentframe()).function
		log.info(f"Func={func} copy of edits starting...")
		start = time.perf_counter()        

		dts_instance = Database.connect_dts_pvcstracker(app_name=self.app_name, log=log)

		# Get Min date from DTS to be used in Diablo query
		qry = '''SELECT
				CAST(MIN(REPLACE(CONVERT(VARCHAR(10),
					RecordingMonth, 121),'-','')) AS INT) AS [Recording Month]
				FROM inventorytracker2..ReleaseList l
				inner join inventorytracker2..ReleaseStatus s
				on s.ReleaseStatusID=l.ReleaseStatusID
				where [Date] >= '2012-11-01' and l.ReleaseStatusId=1'''
		# log.info("GetMinRecordingDate query="+str(qry))
		min_Date = dts_instance.fn_fetch(qry, fetch = 'fetchone', close_conn = False)[0]
		log.info(f"Min Recording Date={str(min_Date)}")

		# Get data from Diablo into dataframe
		diablo_instance = Database.connect_diablo(app_name=self.app_name + ' Edits', log=log)  
		CountyRelEditTotal_records = diablo_instance.fn_populate_dataframe('''
			SELECT 
			t.Cntycd, 
			ds.StateCd + ' ' + ds.CntyName AS [State/County], 
			--CONVERT(VARCHAR(7), 
			--	CONVERT(DATETIME, CONVERT(CHAR(8), t.RecordingDt)),121) 
			concat(left(t.RecordingDt,4),'-',substring(cast(t.RecordingDt as char(8)),5,2)) AS [Recording Month],	
			SUM(Edits) AS Edits, 
			sum(Total) AS Total 
			FROM ( 
			select cntycd, 
			t.RecordingDt, 
			SUM(CASE WHEN EditReqdInd IS NOT NULL THEN 1 ELSE 0 END) AS Edits, 
			COUNT(*) AS Total 
			FROM tTrans.Trans t 
			WHERE ISDATE(t.RecordingDt) = 1 AND t.RecordingDt >= ''' + str(min_Date) + '''
			GROUP BY t.Cntycd, RecordingDt) t 
			LEFT JOIN tcommon.datasupplier ds ON ds.cntycd=t.cntycd 
			GROUP BY t.Cntycd, 
				ds.StateCd, 
				ds.CntyName, 
				concat(left(t.RecordingDt,4),'-',substring(cast(t.RecordingDt as char(8)),5,2))
			--	CONVERT(VARCHAR(7), 
			--		CONVERT(DATETIME, CONVERT(CHAR(8), t.RecordingDt)),121) 
			ORDER BY 1,3 desc
			OPTION (RECOMPILE)''')
		diablo_instance.fn_close()            
		# Load data to tables in DTS
		dts_instance.fn_load_dataframe_to_table(dest_table_name="CountyRelEditTotal", dataframe=CountyRelEditTotal_records, truncate_table = True, commit = True, close_conn = True)

		finish = time.perf_counter()
		log.info(f'Func={func} copy of edits completed, finished in {round(finish-start, 2)} second(s)')


	def copy_rejects(self):

		func = inspect.getframeinfo(inspect.currentframe()).function
		log.info(f"Func={func} copy of rejects starting...")
		start = time.perf_counter()        		

		# Get data from Diablo into dataframe
		diablo_instance = Database.connect_diablo(app_name=self.app_name + ' Rejects', log=log)
		CountyRelRejects_records = diablo_instance.fn_populate_dataframe('''
			SELECT 
				t.CntyCd, 
				ds.StateCd + ' ' + ds.CntyName AS [State/County], 
				--CONVERT(VARCHAR(7), CONVERT(DATETIME, CONVERT(CHAR(8), t.RecordingDt)),121) AS [Recording Month],
				concat(left(t.RecordingDt,4),'-',substring(cast(t.RecordingDt as char(8)),5,2)) AS [Recording Month],
				sum(RejectCount) AS RejectCount
			FROM
			(
			select 
				t.CntyCd,
				RecordingDt,
				Count(1) AS RejectCount
			from tTrans.Trans t
			INNER JOIN (SELECT DISTINCT
								tr.CntyCd,
								BatchDt,
								BatchSeq
						FROM tTrans.TransReject tr
						INNER JOIN Common.vDataSupplier ds ON tr.CntyCd = ds.CntyCd
						WHERE RejectOvrdId IS NULL
						) tr
			ON t.CntyCd = tr.CntyCd
			AND t.BatchDt = tr.BatchDt AND t.BatchSeq = tr.BatchSeq
			WHERE ISDATE(t.RecordingDt) = 1 AND RejectInd = 'Y' AND EditReqdInd IS NOT NULL
			group by t.CntyCd, RecordingDt
			) t
			INNER JOIN Common.vDataSupplier ds ON t.CntyCd = ds.CntyCd
			GROUP BY t.CntyCd,
				ds.StateCd,
				ds.CntyName,
				concat(left(t.RecordingDt,4),'-',substring(cast(t.RecordingDt as char(8)),5,2))
				--CONVERT(VARCHAR(7), CONVERT(DATETIME, CONVERT(CHAR(8), t.RecordingDt)),121)
			ORDER BY 1,3 desc
			OPTION (RECOMPILE)''')
		diablo_instance.fn_close()

		# Load data to table in DTS
		dts_instance = Database.connect_dts_pvcstracker(app_name=self.app_name, log=log)
		dts_instance.fn_load_dataframe_to_table(dest_table_name="CountyRelRejects", dataframe=CountyRelRejects_records, truncate_table = True, commit = True, close_conn = True)

		finish = time.perf_counter()
		log.info(f'Func={func} copy of rejects completed, finished in {round(finish-start, 2)} second(s)')


	def create_reports(self):

		# Create And Email Excel Report
		dts_instance = Database.connect_dts_pvcstracker(app_name=self.app_name, log=log)        
		report_records = dts_instance.fn_populate_dataframe('''
			select
				c.State + ' ' + c.County as [State/County]
				,l.FIPS
				,CONVERT(VARCHAR(10),[Date],121) AS [Date]
				,s.Status
				,CONVERT(VARCHAR(7),RecordingMonth, 121) AS [Recording Month]
				,cet.Edits
				,cet.Total
				,ISNULL(cr.RejectCount,0) as Rejects
				,CAST(cet.Total * 0.02 as numeric) as [2%]
				,CASE WHEN CAST(cet.Total * 0.02 as numeric) > cet.Edits
					THEN 'TRUE'
					ELSE 'FALSE'
				END AS [2% > Edits]
			from inventorytracker2..ReleaseList l
			inner join inventorytracker2..ReleaseStatus s
			on s.ReleaseStatusID=l.ReleaseStatusID
			inner join common.dbo.County c on c.fips=l.fips
			left join CountyRelEditTotal cet
			on cet.Cntycd = l.FIPS and cet.[Recording Month] = CONVERT(VARCHAR(7),l.RecordingMonth, 121)
			left join CountyRelRejects cr
			on cr.CntyCd = l.FIPS and cr.[Recording Month] = CONVERT(VARCHAR(7),l.RecordingMonth, 121)
			where [Date] >= '2012-11-01' and l.ReleaseStatusId=1
			order by 2, 3 desc, 5 desc'''
		)
		dts_instance.fn_close()

		ReportFolder = self.env_config.local_path() + r'\CountyEditReject'
		self.file_util.CheckDirExists(folder=ReportFolder, createdir=True)        
		ReportFile = ReportFolder + (r'\CountiesReleased' + (time.strftime('%Y%m%d')) + r'.xlsx')
		log.info("Report File Name: " + ReportFile)
		report_records.to_excel(ReportFile, index=False)

		log.info("Sending email with attachment " + ReportFile)
		f = self.env_config.get_from_email()
		t = self.app_config.get_parm_value(section=self.app_name,parm="to_email")
		self.email_util.sendemail(
			FromAddress=f,
			ToAddress=t,
			Subject="Counties Released " + time.strftime('%Y%m%d'),
			attachments=[ReportFile]
			)


	def main(self):

		start = time.perf_counter() 
		rc = 0

		try:
			# Copy data concurrently from Diablo to DTS
			with ThreadPoolExecutor(max_workers=2) as executor:
				return_edits = executor.submit(self.copy_edits)
				return_rejects = executor.submit(self.copy_rejects)
			if return_edits.exception() is not None:
				raise return_edits.exception()                
			if return_rejects.exception() is not None:
				raise return_rejects.exception()                                
			self.create_reports()
		except Exception as e:
			log.exception(e, exc_info=e)
			self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
			rc = 1
		else:
			finish = time.perf_counter() 
			log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
		finally:
			exit(rc)


obj = CountyEditReject()
obj.main()
