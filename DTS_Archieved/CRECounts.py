# Initial Author: Byron Centeno
# Date Written: September 16, 2021
# Overview: 1.  Rewrite CRECounts SSIS Package to Python
#           2.  This process
#               a.Executes two sql queries populating temp tables  
#               b.Reads the temp tables into two dataframes  
#               c.Writes dataframes into documentcounts db tables
#               d.Sends notification email  
#
# History: To be completed for each change made after initial release
# Who: When:
# What: ** Include a reference to the JIRA ticket that lead to the change
#
# SQL Server=DTS
# SQL DB=documentcounts
# SQL Server=fulfillment-diablo-dev.infosolco.com
# SQL DB=DiabloSynonyms
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
from utils.ZipUtils import ZipUtils
from vault import vault
from ftp import ftp
# ###################################

log = get_logger('CRECounts')

class CRECounts:
		   
	def __init__(self):
		self.app_name = os.path.splitext(os.path.basename(__file__))[0]        
		log.info(f'{self.app_name} object created...')
		self.env_config = EnvironmentConfig()
		self.app_config = AppConfig()
		self.zip_util = ZipUtils(log=log)
		self.email_util = EmailUtil(log=log)
		self.file_util = FileUtils(log=log)
		self.vault_util = vault(log=log)
   

	def ILUBreakdown(self):

		func = inspect.getframeinfo(inspect.currentframe()).function
		log.info(f"Func={func} extract starting...")
		start = time.perf_counter()

		diablo_fulfillment_instance = Database.connect_diablo_fulfillment(app_name=f'{self.app_name}_ILUBreakdown', log=log)
		query = """        /*Michael's Parcel Count Report - Iris LandUse code breakdown*/

				--Create Spine for Crosstab--
				IF OBJECT_ID ('tempdb.dbo.#COUNTY') IS NOT NULL DROP TABLE #COUNTY
				select p.CntyCd,c.StateCd, c.CntyName,p.IrisLandUseCd
				INTO #COUNTY
				from tTax.Property p WITH (NOLOCK)
				INNER JOIN [tCommon].[DataSupplier] c WITH (NOLOCK)
				ON p.CntyCd = c.CntyCd
				--and p.MUNC_CD = c.MUNC_CD
				and p.IrisLandUseCd IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd,c.StateCd, c.CntyName,p.IrisLandUseCd
				option (recompile)

				--select * from #COUNTY order by cntycd,irislandusecd

				----Get Parcel Counts---- **DO NOT USE per Michael C. as of 7/31/12 **
				--IF OBJECT_ID ('tempdb.dbo.#PARCELS') IS NOT NULL DROP TABLE #PARCELS 
				--select CntyCd,p.IrisLandUseCd, count(PclId) AS [Total Parcel Count]
				--INTO #PARCELS
				--from tTax.Property (NOLOCK)
				--where PclId IS NOT NULL
				--group by CntyCd,p.IrisLandUseCd
				--order by CntyCd,p.IrisLandUseCd
				--option (recompile)
				----select * from #PARCELS



				--/* this query is not needed, NUM_BLDGS table has pcl count*/
				----Get Total Parcels with Iris LandUseCodes--
				IF OBJECT_ID ('tempdb.dbo.#PARCEL_ILU') IS NOT NULL DROP TABLE #PARCEL_ILU
				select p.CntyCd,p.IrisLandUseCd,count(p.PclId) AS [TTL ILU COUNT]
				INTO #PARCEL_ILU
				from tTax.Property p WITH (NOLOCK)
					inner join tTax.Building b WITH (NOLOCK)
					on p.CntyCd = b.CntyCd
					and p.PclId = b.PclId
					and p.PclSeqNbr = b.PclSeqNbr
					--and p.MUNC_CD = b.MUNC_CD
				where p.PclId IS NOT NULL
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and b.BldgSeqNbr = 1
				and p.IrisLandUseCd IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd,p.IrisLandUseCd
				option (recompile)

				--select * from #PARCEL_ILU




				--Get Total Bldgs with Iris LandUseCodes per COMM parcels--
				IF OBJECT_ID ('tempdb.dbo.#NUM_BLDGS') IS NOT NULL DROP TABLE #NUM_BLDGS
				select b.CntyCd,p.IrisLandUseCd, b.PclId,MAX(b.BldgSeqNbr) AS [NUM_OF_BLDG_SEQS]
				INTO #NUM_BLDGS
				from tTax.Building b WITH (NOLOCK)
				INNER JOIN tTax.Property p WITH (NOLOCK)
				ON b.CntyCd = p.CntyCd
				and b.PclId = p.PclId
				and b.PclSeqNbr = p.PclSeqNbr
				where b.PclId IS NOT NULL
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and p.IrisLandUseCd  IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by b.CntyCd,p.IrisLandUseCd,b.PclId
				option (recompile)

				--select * from #NUM_BLDGS

				--Get Total Bldgs from temp table #NUM_BLDGS--
				IF OBJECT_ID ('tempdb.dbo.#TTL_BLDGS_PER_PCL') IS NOT NULL DROP TABLE #TTL_BLDGS_PER_PCL
				select CntyCd,IrisLandUseCd, COUNT(PclId) AS [PCL COUNT],SUM(NUM_OF_BLDG_SEQS) AS [Total Bldgs per Comm Pcls] 
				INTO #TTL_BLDGS_PER_PCL
				from #NUM_BLDGS 
				group by CntyCd,IrisLandUseCd
				order by CntyCd,IrisLandUseCd

				--select * from #TTL_BLDGS_PER_PCL
				----------------------------------------------------------------------


				---------------------------------------------------------------------

				--Get Owner Element Counts--
				IF OBJECT_ID ('tempdb.dbo.#OWNER_NAME') IS NOT NULL DROP TABLE #OWNER_NAME
				select p.CntyCd,p.IrisLandUseCd
				,count(pn.IrisName1) AS [Owner Name]
				,count(AddrHse1Nbr) AS [Mailing Address House Number]
				,count(AddrStreetName) AS [Mailing Address Street Name]
				,count(OrigCityName) AS [Mailing Address City]
				,count(OrigStCd) AS [Mailing Address State]
				,count(OrigZipCd) AS [Mailing Address Zip]
				INTO #OWNER_NAME
				from tTax.Property p WITH (NOLOCK)
					INNER JOIN tTax.PropertyLegalParty pn WITH (NOLOCK)
					on p.CntyCd = pn.CntyCd
					--and p.MUNC_CD = pn.MUNC_CD /* need to see if it is brought into Diablo */
					and p.PclId = pn.PclId
					and p.PclSeqNbr = pn.PclSeqNbr
					INNER JOIN tTax.Building b WITH (NOLOCK)
					ON b.CntyCd = p.CntyCd
					--and b.MUNC_CD = p.MUNC_CD  /* need to see if it is brought into Diablo */
					and b.PclId = p.PclId
					and b.PclSeqNbr = p.PclSeqNbr
				where p.PclId IS NOT NULL      --need to check if diablo uses this for pending/rejected records
				and pn.nameTypId =  2   --- OWNR
				--and pn.nameseq = 1
				and pn.IrisName1 is not null
					and b.BldgSeqNbr = 1 
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and p.IrisLandUseCd IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd,p.IrisLandUseCd
				option (recompile)
				--select * from #OWNER_NAME

				------------------------------------------------------------------

				--Get Owner Name Corporate Indicator Element--
				IF OBJECT_ID ('tempdb.dbo.#OWNER_CORP_IND') IS NOT NULL DROP TABLE #OWNER_CORP_IND
				select p.CntyCd,p.IrisLandUseCd,count(pn.IrisCorpInd) AS [Owner Name Corp Ind]
				INTO #OWNER_CORP_IND
				from tTax.Property p WITH (NOLOCK)
					left JOIN tTax.PropertyLegalParty pn WITH (NOLOCK)
					on p.CntyCd = pn.CntyCd
					--and p.MUNC_CD = pn.MUNC_CD
					and p.PclId = pn.PclId
					and p.PclSeqNbr = pn.PclSeqNbr
					INNER JOIN tTax.Building b WITH (NOLOCK)
					ON b.CntyCd = p.CntyCd
					--and b.MUNC_CD = p.MUNC_CD
					and b.PclId = p.PclId
					and b.PclSeqNbr = p.PclSeqNbr
				where p.PclId IS NOT NULL and pn.IrisCorpInd is not null
				and pn.NameTypId = 2   ---OWNR
				and pn.IrisName1 is not null
				and b.BldgSeqNbr = 1 
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and p.IrisLandUseCd  IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd,p.IrisLandUseCd
				option (recompile)

				--select * from #OWNER_CORP_IND



				------------------------------------------------------------------

				--Get Building Elements Counts--
				IF OBJECT_ID ('tempdb.dbo.#BLD_ELEMENTS') IS NOT NULL DROP TABLE #BLD_ELEMENTS
				select p.CntyCd,p.IrisLandUseCd, COUNT(b.StoriesNbr) AS [Stories],COUNT(b.UnvAcCd) AS [Air Conditioning]
				,COUNT(b.UnvPrkgCd) AS [Parking Type],count(b.GarageCd) AS [Garage Type], COUNT(b.GarageSqFtNbr) AS [Garage Square Footage]
				,COUNT(b.BsmtSqFtNbr) AS Basement, COUNT(b.GroundFlrSqFtNbr) AS [Ground Floor Square Footage]
				,COUNT(b.GrossSqFtNbr) AS [Gross Building Square Footage],COUNT(AdjGrossSqFtNbr) AS [Adj Gross Building Square Foot]
				,COUNT(BldgSqFtTotalNbr) AS [Building Square Footage],COUNT(LivSqFtNbr) AS [Living Square Footage]
				,COUNT(YyBltActDt) AS [Year Built Actual],COUNT(YyBltEffDt) [Year Build Effective],COUNT(BldgCd) AS [Bldg Code]
				,COUNT(PrkgActlSpaceNbr) AS [Parking Spaces],COUNT(CommUnitsNbr) AS [Number of Units (Comm)]
				,COUNT(ResUnitsNbr) AS [Number of Units (Resid)]
				INTO #BLD_ELEMENTS
				from tTax.Building b WITH (NOLOCK)
					INNER JOIN tTax.Property p WITH (NOLOCK)
					on p.CntyCd = b.CntyCd
					--and p.MUNC_CD = b.MUNC_CD
					and p.PclId = b.PclId
					and p.PclSeqNbr = b.PclSeqNbr
				where p.PclId is not null
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and p.IrisLandUseCd IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd,p.IrisLandUseCd
				option (recompile)
				--select * from #BLD_ELEMENTS

				------------------------------------------------------------------

				--Get Property Elements Counts--
				IF OBJECT_ID ('tempdb.dbo.#PROP_ELEMENTS') IS NOT NULL DROP TABLE #PROP_ELEMENTS
				select p.CntyCd,p.IrisLandUseCd, COUNT(LandUseStCd) AS [Land Use (State)],count(LandUseCntyCd) AS [Land Use (County)]
				,COUNT(PropUseCd) AS [Property Use],COUNT(StdSubdCd) AS [Subdivision/Condo Code],COUNT(LandDimAcresTotal) AS [Land Acres]
				,COUNT(LandDimSqFtTotal) AS [Land Square Footage],COUNT(SitusAddrHse1Nbr) AS [Situs Address House Number]
				,COUNT(SitusAddrStreetName) AS [Situs Address Street Name],COUNT(SitusOrigCityName) AS [Situs City Name],COUNT(SitusOrigZipCd) AS [Situs Address Zip Code]
				,COUNT(TotalTaxAmt) AS [Tax Total Amount],COUNT(AssdLandValTotal) AS [Assessed Land Value]
				,COUNT(AssdImpvValTotal) AS [Assessed Improvement Value],COUNT(AssdTotalValAmt) AS [Assessed Total Value]
				,COUNT(MktLandValTotal) AS [Market Land Value],COUNT(MktImpvValTotal) AS [Market Improvement Value],count(MktTotalValAmt) AS [Market Total Value]
				,COUNT(ApprLandValTotal) AS [Appraised Land Value],COUNT(ApprImpvValTotal) AS [Appraised Improvement Value]
				,COUNT(ApprTotalValAmt) AS [Appraised Total Value],COUNT(ZnCd) AS [Zoning Code]
				,COUNT(SumNbrUnits) AS [Number of Units (Sum)]
				INTO #PROP_ELEMENTS
				from tTax.Property p WITH (NOLOCK)
					INNER JOIN tTax.Building b WITH (NOLOCK)
					ON b.CntyCd = p.CntyCd
					--and b.MUNC_CD = p.MUNC_CD
					and b.PclId = p.PclId
					and b.PclSeqNbr = p.PclSeqNbr
				where p.PclId is not null
				and  (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and b.BldgSeqNbr = 1 
				and p.IrisLandUseCd  IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd,p.IrisLandUseCd
				option (recompile)

				--select * from #PROP_ELEMENTS



				--Aggregate Report - Parcel Counts
				IF OBJECT_ID ('Temporary..CRE_ILU_Breakdown') IS NOT NULL DROP TABLE Temporary..CRE_ILU_Breakdown
				SELECT
				c.CntyCd FIPS
				,c.StateCd AS [State]
				,c.CntyName AS County
				,c.IrisLandUseCd as [ILU Code]
				, (select [TTL ILU COUNT] from #PARCEL_ILU ilu where ilu.CntyCd = c.CntyCd and ilu.IrisLandUseCd = c.IrisLandUseCd) [Total Parcel for COMM ILU]
				, (select [Total Bldgs per Comm Pcls] from  #TTL_BLDGS_PER_PCL ttb where ttb.CntyCd = c.CntyCd and ttb.IrisLandUseCd = c.IrisLandUseCd) [Total Bldgs per Comm Pcls]
				, (select [Owner Name] from #OWNER_NAME o where o.CntyCd = c.CntyCd and o.IrisLandUseCd = c.IrisLandUseCd) [Owner Name] 
				, (select [Mailing Address House Number] from #OWNER_NAME o where o.CntyCd = c.CntyCd and o.IrisLandUseCd = c.IrisLandUseCd) [Mailing Address House Number]
				, (select [Mailing Address Street Name] from #OWNER_NAME o where o.CntyCd = c.CntyCd and o.IrisLandUseCd = c.IrisLandUseCd) [Mailing Address Street Name]
				, (select [Mailing Address City] from #OWNER_NAME o where o.CntyCd = c.CntyCd and o.IrisLandUseCd = c.IrisLandUseCd) [Mailing Address City]
				, (select [Mailing Address State] from #OWNER_NAME o where o.CntyCd = c.CntyCd and o.IrisLandUseCd = c.IrisLandUseCd) [Mailing Address State]
				, (select [Mailing Address Zip] from #OWNER_NAME o where o.CntyCd = c.CntyCd and o.IrisLandUseCd = c.IrisLandUseCd) [Mailing Address Zip]
				, (select [Owner Name Corp Ind] from #OWNER_CORP_IND oi where oi.CntyCd = c.CntyCd and oi.IrisLandUseCd = c.IrisLandUseCd) [Owner Name Corp Ind]
				, (select [Land Use (State)] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Land Use (State)]
				, (select [Land Use (County)] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Land Use (County)]
				, (select [Property Use] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Property Use]
				, (select [Subdivision/Condo Code] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Subdivision/Condo Code]
				, (select [Land Acres] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Land Acres]
				, (select [Land Square Footage] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Land Square Footage]
				, (select [Situs Address House Number] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Situs Address House Number]
				, (select [Situs Address Street Name] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Situs Address Street Name]
				, (select [Situs City Name] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Situs City Name]
				, (select [Situs Address Zip Code] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Situs Address Zip Code]
				, (select [Tax Total Amount] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Tax Total Amount]
				, (select [Assessed Land Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Assessed Land Value]
				, (select [Assessed Improvement Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Assessed Improvement Value]
				, (select [Assessed Total Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Assessed Total Value]
				, (select [Market Land Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Market Land Value]
				, (select [Market Improvement Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Market Improvement Value]
				, (select [Market Total Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Market Total Value]
				, (select [Appraised Land Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Appraised Land Value]
				, (select [Appraised Improvement Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Appraised Improvement Value]
				, (select [Appraised Total Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Appraised Total Value]
				, (select [Zoning Code] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Zoning Code]
				, (select [Number of Units (Sum)] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd and pe.IrisLandUseCd = c.IrisLandUseCd) [Number of Units (Sum)]
				, (select [Stories] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Stories]
				, (select [Air Conditioning] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Air Conditioning]
				, (select [Parking Type] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Parking Type]
				, (select [Garage Type] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Garage Type]
				, (select [Garage Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Garage Square Footage]
				, (select [Basement] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Basement]
				, (select [Ground Floor Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Ground Floor Square Footage]
				, (select [Gross Building Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Gross Building Square Footage]
				, (select [Adj Gross Building Square Foot] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Adj Gross Building Square Foot]
				, (select [Building Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Building Square Footage]
				, (select [Living Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Living Square Footage]
				, (select [Year Built Actual] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Year Built Actual]
				, (select [Year Build Effective] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Year Build Effective]
				, (select [Bldg Code] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Bldg Code]
				, (select [Parking Spaces] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Parking Spaces]
				, (select [Number of Units (Comm)] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Number of Units (Comm)]
				, (select [Number of Units (Resid)] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd and be.IrisLandUseCd = c.IrisLandUseCd) [Number of Units (Resid)]
				INTO Temporary..CRE_ILU_Breakdown
				from #COUNTY c
				order by 1,2,3,4
				"""
		diablo_fulfillment_instance.fn_execute(query=query, commit=True, close_conn=False)

		CRE_ILU_Breakdown_sql = 'select convert(datetime,convert(varchar(10), GETDATE(), 121)) as [Import Date], * from Temporary..CRE_ILU_Breakdown order by 2,3,4,5'
		ILU_Breakdown_destTable = '[dbo].[IluComm]'

		Breakdown_df = diablo_fulfillment_instance.fn_populate_dataframe(query=CRE_ILU_Breakdown_sql)
		Breakdown_df.rename(columns = {'Subdivision/Condo Code':'Subdivision Condo Code'}, inplace = True)

		diablo_fulfillment_instance.fn_close()

		dts_documentcounts_instance = Database.connect_dts_documentcounts(app_name=f'{self.app_name}_ILUBreakdown', log=log)
		dts_documentcounts_instance.fn_load_dataframe_to_table(dest_table_name=ILU_Breakdown_destTable , dataframe=Breakdown_df, truncate_table=False, commit=True, close_conn=True, cnvrt_to_none=True)

		finish = time.perf_counter()
		log.info(f'Func={func} copy completed, finished in {round(finish-start, 2)} second(s)')
		
		return

		
	def Aggregate(self):

		func = inspect.getframeinfo(inspect.currentframe()).function
		log.info(f"Func={func} extract starting...")
		start = time.perf_counter()

		diablo_fulfillment_instance = Database.connect_diablo_fulfillment(app_name=f'{self.app_name}_Aggregate', log=log)

		Agg_sql = """ /*Michael's Parcel Count Aggregate Report*/

				--Create Spine for Crosstab--
				IF OBJECT_ID ('tempdb.dbo.#COUNTY') IS NOT NULL DROP TABLE #COUNTY
				select p.CntyCd,c.StateCd, c.CntyName
				INTO #COUNTY
				from tTax.Property p WITH (NOLOCK)
					INNER JOIN [tCommon].[DataSupplier] c WITH (NOLOCK)
					ON p.CntyCd = c.CntyCd
					--and p.MUNC_CD = c.MUNC_CD
						--WHERE p.CntyCd = '06059'
				group by p.CntyCd,c.StateCd, c.CntyName
				option (recompile)

				--select * from #COUNTY

				--Get Parcel Counts--
				IF OBJECT_ID ('tempdb.dbo.#PARCELS') IS NOT NULL DROP TABLE #PARCELS 
				select CntyCd, count(PclId) AS [Total Parcel Count]
				INTO #PARCELS
				from tTax.Property WITH (NOLOCK)
				where PclId IS NOT NULL
				--and CntyCd = '06059'
				group by CntyCd
				option (recompile)

				--select * from #PARCELS

				----Get Total Parcels with Iris LandUseCodes--
				IF OBJECT_ID ('tempdb.dbo.#PARCEL_ILU') IS NOT NULL DROP TABLE #PARCEL_ILU
				select p.CntyCd,count(p.PclId) AS [TTL ILU COUNT]
				INTO #PARCEL_ILU
				from tTax.Property p WITH (NOLOCK)
					inner join tTax.Building b WITH (NOLOCK)
					on p.CntyCd = b.CntyCd
					and p.PclId = b.PclId
					and p.PclSeqNbr = b.PclSeqNbr
					--and p.MUNC_CD = b.MUNC_CD
				where p.PclId IS NOT NULL
					--and p.CntyCd = '06059'
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and b.BldgSeqNbr = 1
				and p.IrisLandUseCd IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd
				option (recompile)

				--select * from #PARCEL_ILU

				--Get Total Bldgs with Iris LandUseCodes per COMM parcels--
				IF OBJECT_ID ('tempdb.dbo.#NUM_BLDGS') IS NOT NULL DROP TABLE #NUM_BLDGS
				select b.CntyCd, b.PclId,MAX(b.BldgSeqNbr) AS [NUM_OF_BLDG_SEQS]
				INTO #NUM_BLDGS
				from tTax.Building b WITH (NOLOCK)
				INNER JOIN tTax.Property p WITH (NOLOCK)
				ON b.CntyCd = p.CntyCd
				and b.PclId = p.PclId
				and b.PclSeqNbr = p.PclSeqNbr
				where b.PclId IS NOT NULL
					--and p.CntyCd = '06059'
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and p.IrisLandUseCd  IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by b.CntyCd,b.PclId
				option (recompile)

				--select * from #NUM_BLDGS

				--Get Total Bldgs from temp table #NUM_BLDGS--
				IF OBJECT_ID ('tempdb.dbo.#TTL_BLDGS_PER_PCL') IS NOT NULL DROP TABLE #TTL_BLDGS_PER_PCL
				select CntyCd, COUNT(PclId) AS [PCL COUNT],SUM(NUM_OF_BLDG_SEQS) AS [Total Bldgs per Comm Pcls] 
				INTO #TTL_BLDGS_PER_PCL
				from #NUM_BLDGS 
				group by CntyCd

				--select * from #TTL_BLDGS_PER_PCL				

				--Get Owner Element Counts--
				IF OBJECT_ID ('tempdb.dbo.#OWNER_NAME') IS NOT NULL DROP TABLE #OWNER_NAME
				select p.CntyCd
				,count(pn.IrisName1) AS [Owner Name]
				,count(AddrHse1Nbr) AS [Mailing Address House Number]
				,count(AddrStreetName) AS [Mailing Address Street Name]
				,count(OrigCityName) AS [Mailing Address City]
				,count(OrigStCd) AS [Mailing Address State]
				,count(OrigZipCd) AS [Mailing Address Zip]
				INTO #OWNER_NAME
				from tTax.Property p WITH (NOLOCK)
				INNER JOIN tTax.PropertyLegalParty pn WITH (NOLOCK)
					on p.CntyCd = pn.CntyCd
					--and p.MUNC_CD = pn.MUNC_CD /* need to see if it is brought into Diablo */
					and p.PclId = pn.PclId
					and p.PclSeqNbr = pn.PclSeqNbr
					INNER JOIN tTax.Building b WITH (NOLOCK)
					ON b.CntyCd = p.CntyCd
					--and b.MUNC_CD = p.MUNC_CD  /* need to see if it is brought into Diablo */
					and b.PclId = p.PclId
					and b.PclSeqNbr = p.PclSeqNbr
				where p.PclId IS NOT NULL      --need to check if diablo uses this for pending/rejected records
				and pn.nameTypId =  '2'   --- OWNR
				--and pn.nameseq = 1
				and pn.IrisName1 is not null
					--and p.CntyCd = '06059'
					and b.BldgSeqNbr = 1 
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and p.IrisLandUseCd IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd
				option (recompile)

				--select * from #OWNER_NAME

				------------------------------------------------------------------

				--Get Owner Name Corporate Indicator Element--
				IF OBJECT_ID ('tempdb.dbo.#OWNER_CORP_IND') IS NOT NULL DROP TABLE #OWNER_CORP_IND
				select p.CntyCd,count(pn.IrisCorpInd) AS [Owner Name Corp Ind]
				INTO #OWNER_CORP_IND
				from tTax.Property p WITH (NOLOCK)
					left JOIN tTax.PropertyLegalParty pn WITH (NOLOCK)
					on p.CntyCd = pn.CntyCd
					--and p.MUNC_CD = pn.MUNC_CD
					and p.PclId = pn.PclId
					and p.PclSeqNbr = pn.PclSeqNbr
					INNER JOIN tTax.Building b WITH (NOLOCK)
					ON b.CntyCd = p.CntyCd
					--and b.MUNC_CD = p.MUNC_CD
					and b.PclId = p.PclId
					and b.PclSeqNbr = p.PclSeqNbr
				where p.PclId IS NOT NULL and pn.IrisCorpInd is not null
				and pn.NameTypId = '2'   ---OWNR
				and pn.IrisName1 is not null
					--and p.CntyCd = '06059'
				and b.BldgSeqNbr = 1 
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and p.IrisLandUseCd  IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd
				option (recompile)

				--select * from #OWNER_CORP_IND

				--Get Building Elements Counts--
				IF OBJECT_ID ('tempdb.dbo.#BLD_ELEMENTS') IS NOT NULL DROP TABLE #BLD_ELEMENTS
				select p.CntyCd, COUNT(b.StoriesNbr) AS [Stories],COUNT(b.UnvAcCd) AS [Air Conditioning]
				,COUNT(b.UnvPrkgCd) AS [Parking Type],count(b.GarageCd) AS [Garage Type], COUNT(b.GarageSqFtNbr) AS [Garage Square Footage]
				,COUNT(b.BsmtSqFtNbr) AS Basement, COUNT(b.GroundFlrSqFtNbr) AS [Ground Floor Square Footage]
				,COUNT(b.GrossSqFtNbr) AS [Gross Building Square Footage],COUNT(AdjGrossSqFtNbr) AS [Adj Gross Building Square Foot]
				,COUNT(BldgSqFtTotalNbr) AS [Building Square Footage],COUNT(LivSqFtNbr) AS [Living Square Footage]
				,COUNT(YyBltActDt) AS [Year Built Actual],COUNT(YyBltEffDt) [Year Build Effective],COUNT(BldgCd) AS [Bldg Code]
				,COUNT(PrkgActlSpaceNbr) AS [Parking Spaces],COUNT(CommUnitsNbr) AS [Number of Units (Comm)]
				,COUNT(ResUnitsNbr) AS [Number of Units (Resid)]
				INTO #BLD_ELEMENTS
				from tTax.Building b WITH (NOLOCK)
					INNER JOIN tTax.Property p WITH (NOLOCK)
					on p.CntyCd = b.CntyCd
					--and p.MUNC_CD = b.MUNC_CD
					and p.PclId = b.PclId
					and p.PclSeqNbr = b.PclSeqNbr
				where p.PclId is not null
				and (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
					--and p.CntyCd = '06059'
				and p.IrisLandUseCd IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')
				group by p.CntyCd
				option (recompile)
				--select * from #BLD_ELEMENTS

				------------------------------------------------------------------

				--Get Property Elements Counts--
				IF OBJECT_ID ('tempdb.dbo.#PROP_ELEMENTS') IS NOT NULL DROP TABLE #PROP_ELEMENTS
				select p.CntyCd, COUNT(LandUseStCd) AS [Land Use (State)],count(LandUseCntyCd) AS [Land Use (County)]
				,COUNT(PropUseCd) AS [Property Use],COUNT(StdSubdCd) AS [Subdivision/Condo Code],COUNT(LandDimAcresTotal) AS [Land Acres]
				,COUNT(LandDimSqFtTotal) AS [Land Square Footage],COUNT(SitusAddrHse1Nbr) AS [Situs Address House Number]
				,COUNT(SitusAddrStreetName) AS [Situs Address Street Name],COUNT(SitusOrigCityName) AS [Situs City Name],COUNT(SitusOrigZipCd) AS [Situs Address Zip Code]
				,COUNT(TotalTaxAmt) AS [Tax Total Amount],COUNT(AssdLandValTotal) AS [Assessed Land Value]
				,COUNT(AssdImpvValTotal) AS [Assessed Improvement Value],COUNT(AssdTotalValAmt) AS [Assessed Total Value]
				,COUNT(MktLandValTotal) AS [Market Land Value],COUNT(MktImpvValTotal) AS [Market Improvement Value],count(MktTotalValAmt) AS [Market Total Value]
				,COUNT(ApprLandValTotal) AS [Appraised Land Value],COUNT(ApprImpvValTotal) AS [Appraised Improvement Value]
				,COUNT(ApprTotalValAmt) AS [Appraised Total Value],COUNT(ZnCd) AS [Zoning Code]
				,COUNT(SumNbrUnits) AS [Number of Units (Sum)]
				INTO #PROP_ELEMENTS
				from tTax.Property p WITH (NOLOCK)
					INNER JOIN tTax.Building b WITH (NOLOCK)
					ON b.CntyCd = p.CntyCd
					--and b.MUNC_CD = p.MUNC_CD
					and b.PclId = p.PclId
					and b.PclSeqNbr = p.PclSeqNbr
				where p.PclId is not null
						--and p.CntyCd = '06059' 
				and  (p.UnvImpvValAmt >0
				or b.UnvBldgSqFtTotalNbr is not null
				or b.YyBltActDt is not null)
				and b.BldgSeqNbr = 1 
				and p.IrisLandUseCd  IN ('500','505','703','706','709','510','235','712','512'
				,'514','214','727','515','520','511','530','533','540','742','745','542','750'
				,'230','234','239','754','798','501','755','237','238','502','550','757','560'
				,'562','766','769','700','775','780','787','784','790','797','570','575','200'
				,'103','127','142','130','302','303','304','308','301','311','312','316','318'
				,'320','300','321','322','323','324','328','326','331','349','334','333','336'
				,'338','313','342','310','344','352','354','356','358','361','353','309','364'
				,'366','106','155','134','135','136','132','131','133','151','167','199','165'
				,'209','213','248','245','246','244','247','276','203','204','205','243','724'
				,'721','210','725','733','217','795','221','284','229','240','242','257','261'
				,'262','282','266','268','270','278','220','279','281','273','283','285','799'
				,'286')

				group by p.CntyCd
				option (recompile)

				--select * from #PROP_ELEMENTS

				--Aggregate Report - Parcel Counts
				IF OBJECT_ID ('Temporary..CRE_Luse_Counts') IS NOT NULL DROP TABLE Temporary..CRE_Luse_Counts
				SELECT
				c.CntyCd FIPS
				,c.StateCd AS [State]
				,c.CntyName AS County
				, (select [Total Parcel Count] from #PARCELS c1 WHERE c1.CntyCd = c.CntyCd) [Total Parcel Count]
				, (select [TTL ILU COUNT] from #PARCEL_ILU ilu where ilu.CntyCd = c.CntyCd) [Total Parcel for COMM ILU]
				, (select [Total Bldgs per Comm Pcls] from  #TTL_BLDGS_PER_PCL ttb where ttb.CntyCd = c.CntyCd ) [Total Bldgs per Comm Pcls]
				, (select [Owner Name] from #OWNER_NAME o where o.CntyCd = c.CntyCd) [Owner Name] 
				, (select [Mailing Address House Number] from #OWNER_NAME o where o.CntyCd = c.CntyCd) [Mailing Address House Number]
				, (select [Mailing Address Street Name] from #OWNER_NAME o where o.CntyCd = c.CntyCd) [Mailing Address Street Name]
				, (select [Mailing Address City] from #OWNER_NAME o where o.CntyCd = c.CntyCd) [Mailing Address City]
				, (select [Mailing Address State] from #OWNER_NAME o where o.CntyCd = c.CntyCd) [Mailing Address State]
				, (select [Mailing Address Zip] from #OWNER_NAME o where o.CntyCd = c.CntyCd) [Mailing Address Zip]
				, (select [Owner Name Corp Ind] from #OWNER_CORP_IND oi where oi.CntyCd = c.CntyCd) [Owner Name Corp Ind]
				, (select [Land Use (State)] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Land Use (State)]
				, (select [Land Use (County)] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Land Use (County)]
				, (select [Property Use] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Property Use]
				, (select [Subdivision/Condo Code] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Subdivision/Condo Code]
				, (select [Land Acres] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Land Acres]
				, (select [Land Square Footage] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Land Square Footage]
				, (select [Situs Address House Number] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Situs Address House Number]
				, (select [Situs Address Street Name] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Situs Address Street Name]
				, (select [Situs City Name] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Situs City Name]
				, (select [Situs Address Zip Code] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Situs Address Zip Code]
				, (select [Tax Total Amount] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Tax Total Amount]
				, (select [Assessed Land Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Assessed Land Value]
				, (select [Assessed Improvement Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Assessed Improvement Value]
				, (select [Assessed Total Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Assessed Total Value]
				, (select [Market Land Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Market Land Value]
				, (select [Market Improvement Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Market Improvement Value]
				, (select [Market Total Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Market Total Value]
				, (select [Appraised Land Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Appraised Land Value]
				, (select [Appraised Improvement Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Appraised Improvement Value]
				, (select [Appraised Total Value] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Appraised Total Value]
				, (select [Zoning Code] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Zoning Code]
				, (select [Number of Units (Sum)] from #PROP_ELEMENTS pe where pe.CntyCd = c.CntyCd) [Number of Units (Sum)]
				, (select [Stories] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Stories]
				, (select [Air Conditioning] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Air Conditioning]
				, (select [Parking Type] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Parking Type]
				, (select [Garage Type] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Garage Type]
				, (select [Garage Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Garage Square Footage]
				, (select [Basement] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Basement]
				, (select [Ground Floor Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Ground Floor Square Footage]
				, (select [Gross Building Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Gross Building Square Footage]
				, (select [Adj Gross Building Square Foot] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Adj Gross Building Square Foot]
				, (select [Building Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Building Square Footage]
				, (select [Living Square Footage] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Living Square Footage]
				, (select [Year Built Actual] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Year Built Actual]
				, (select [Year Build Effective] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Year Build Effective]
				, (select [Bldg Code] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Bldg Code]
				, (select [Parking Spaces] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Parking Spaces]
				, (select [Number of Units (Comm)] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Number of Units (Comm)]
				, (select [Number of Units (Resid)] from #BLD_ELEMENTS be where be.CntyCd = c.CntyCd) [Number of Units (Resid)]
				INTO Temporary..CRE_Luse_Counts
				from #COUNTY c
				order by 1,2,3
					"""
		diablo_fulfillment_instance.fn_execute(Agg_sql, commit=True, close_conn=False)  
		
		CRE_Luse_Counts_sql = 'select convert(datetime,convert(varchar(10), GETDATE(), 121)) as [Import Date], * from Temporary..CRE_Luse_Counts order by 2,3,4'
		ILU_CommAggregate_DestTable = '[dbo].[IluCommAggregate]'

		LuseCounts_df = diablo_fulfillment_instance.fn_populate_dataframe(query=CRE_Luse_Counts_sql)
		LuseCounts_df.rename(columns = {'Subdivision/Condo Code':'Subdivision Condo Code'}, inplace = True)

		diablo_fulfillment_instance.fn_close()

		dts_documentcounts_instance = Database.connect_dts_documentcounts(app_name=f'{self.app_name}_Aggregate', log=log)
		dts_documentcounts_instance.fn_load_dataframe_to_table(ILU_CommAggregate_DestTable, LuseCounts_df, truncate_table=False, commit=True, close_conn=True, cnvrt_to_none=True)

		finish = time.perf_counter()
		log.info(f'Func={func} copy completed, finished in {round(finish-start, 2)} second(s)')
		
		return



	def EmailNotification(self):
		
		log.info('Sending notification email...')
		From_Email = self.env_config.get_from_email()
		To_Address = self.app_config.get_parm_value(section=self.app_name,parm="to_email")		
		self.email_util.sendemail(
			FromAddress=From_Email,
			ToAddress=To_Address,
			Subject=f'{self.app_name}' + time.strftime('%Y%m%d')
			)     
		
		
	def main(self):

		start = time.perf_counter() 
		rc = 0

		try:
			# Create the 2 extracts in parallel
			with ThreadPoolExecutor(max_workers=3) as executor:
				p1 = executor.submit(self.ILUBreakdown)
				p2 = executor.submit(self.Aggregate)            
			if p1.exception() is not None:
				raise p1.exception()
			if p2.exception() is not None:
				raise p2.exception()
			self.EmailNotification()
		except Exception as e:
			log.exception(e, exc_info=e)
			self.email_util.send_failure(app_name=self.app_name, error_message=e, filename=[log.handlers[0].baseFilename])
			rc = 1
		else:
			finish = time.perf_counter() 
			log.info(f'---Process successfully completed, Elapsed: {round(finish-start, 2)} second(s)')
		finally:
			exit(rc)  


obj = CRECounts()
obj.main()

		




