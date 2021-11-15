from collections import deque
# from typing import Deque
from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport
# from configs.ConfigUtility import AppConfig
# from utils import get_logger
# from configs import EnvironmentConfig
# from urllib.parse import urlencode
# import sys

# env_prop = EnvironmentConfig()

# log = get_logger('AddressPlus')

class AddressPlus():

    # def __init__(self):
    #     self.app_obj = AppConfig()

    @staticmethod
    # def addrplus(fulladdress: str, city: str, state: str, zipcode: str, country: str, foreignlastline: str, addresstype: str = 'S', log = None):
    def addrplus(fulladdress: str, city: str, state: str, zipcode: str, country: str, foreignlastline: str, addresstype: str = 'S'):
        # if log is None:
        #     log = get_logger('AddressPlus')
        cols_value_maxlen = {'ErrorCode': 4, 'MatchCode': 4, 'LocCode': 4, 'Postalized': 3, 'IsForeign': 3, 'Parcel': 60, 'ParcelSeq': 3, 'Number': 10, 'NumberTo': 10, 'Fraction': 10, 'Predir': 2, 'Street': 30, 'Suffix': 5, 'Postdir': 2, 'Unitnumber': 10, 'PsdNumberPrefix': 5, 'PsdNumber': 10, 'PsdNumberFraction': 10, 'PsdNumberTo': 10, 'FullAddress': 60, 'City': 40, 'State': 2, 'Country': 30, 'ForeignCSZ': 60, 'ForeignLastLine': 60, 'ForeignLabel': 60, 'ZIPCode': 5, 'ZIP4': 4, 'CRRT': 4, 'CRRTZone': 10, 'DPBC': 2, 'Latitude': 10, 'Longitude': 11, 'RBDI': 1, 'CBSA': 5, 'MSA': 4, 'LOT': 4, 'LOTOrder': 1, 'CensusTract': 6, 'CensusBlockFull': 4, 'CensusBlockGroup': 1, 'CensusBlock2': 2, 'CensusBlockSuffix': 1, 'MapPageGrid': 15, 'DPVCMRA': 2, 'DPVConfirm': 2, 'DPVFootnote1': 3, 'DPVFootnote2': 3, 'DPVVacant': 1, 'LACSLinkInd': 2, 'FIPS': 5, 'FIPSState': 2, 'FIPSCounty': 3, 'ProcessCode': 4, 'CountyName': 60}

        try:
            wsdl = 'http://addressplus.corelogic.com:14100/addressplus/addrplus.wsdl'
            transport = Transport(timeout=10, cache=SqliteCache())
            settings = Settings(strict=False)
            client = Client(
                wsdl,
                settings=settings,
                transport=transport,
                service_name='AddressPlusService',
                port_name='AddressPlusPort'
                )
            response = client.service.CLStandardizeAddress(
                Version='1.0',
                User='cmasuser',
                Password='cmadr8in',
                Config='CMAS1',
                Address=fulladdress,
                City=city,
                State=state,
                ZIPCode=zipcode,
                Country=country,
                ForeignLastLine=foreignlastline,
                AddressType=addresstype
                )
        except Exception as e:
            # log.exception(e, exc_info=e)
            # exit(1)
            raise
            
        online_data = { 'SitusMatchCd': '', 'SitusStdCntyCd': '', 'SitusStdAddr': '', 'SitusStdHse1Nbr': '', 'SitusAddrSfx1Cd': '', 'SitusAddrDirLeftCd': '', 'SitusAddrStreetName': '', 'SitusAddrModeCd': '', 'SitusAddrDirRightCd': '', 'SitusAddrAptNbr': '', 'SitusStdCityName': '', 'SitusStdStCd': '', 'SitusStdZipCd': '', 'SitusCensId': '', 'SitusLatDegr': '', 'SitusLongDegr': '', 'SitusGeoMatchCd': '', 'SitusCbsaCd': '' }
        raw_data = {}
      
        ''' Initialize type None to str '''
        for i, k in enumerate(response):
            if isinstance(response[k], deque):
                continue
            if not isinstance(response[k], type(None)):
                response[k] = response[k].strip()
            else:
                response[k] = ''
            len = cols_value_maxlen[k] if cols_value_maxlen.get(k) is not None else 100
            raw_data[k] = response[k][0:len]

        ''' addressPlus match code if address is blank or null '''
        if response['ErrorCode'] > '':
            online_data['SitusMatchCd'] = response['ErrorCode']
        else:
            online_data['SitusMatchCd'] = response['MatchCode']
            online_data['SitusGeoMatchCd'] = response['LocCode']
            online_data['SitusStdCntyCd'] = response['FIPS']
            online_data['SitusStdAddr'] = response['FullAddress']
            online_data['SitusStdHse1Nbr'] = response['Number']
            online_data['SitusAddrSfx1Cd'] = response['Fraction']
            online_data['SitusAddrDirLeftCd'] = response['Predir']
            online_data['SitusAddrStreetName'] = response['Street'][0:30]
            online_data['SitusAddrModeCd'] = response['Suffix']
            online_data['SitusAddrDirRightCd'] = response['Postdir']
            online_data['SitusAddrAptNbr'] = response['Unitnumber'][0:10]
            online_data['SitusStdCityName'] = response['City']
            online_data['SitusStdStCd'] = response['State']
            if response['ZIPCode'] > '':
                online_data['SitusStdZipCd'] = response['ZIPCode'][0:5] + response['ZIP4'][0:4]
            if response['CensusTract'] > '':
                online_data['SitusCensId'] = response['CensusTract'][0:6] + response['CensusBlockFull'][0:4]
            online_data['SitusLatDegr'] = response['Latitude']
            online_data['SitusLongDegr'] = response['Longitude']
            online_data['SitusCbsaCd'] = response['CBSA']

        d = deque(response._raw_elements)
        for elem in d:      # iterate over the deque's elements
            if isinstance(elem.text, type(None)):
                continue
            tag = elem.tag.replace('{urn:AddressPlus}', '')
            value = elem.text.strip()
            len = cols_value_maxlen[tag] if cols_value_maxlen.get(tag) is not None else 100
            raw_data[tag] = value[0:len]
            # if tag == 'CountyName':
            #     raw_data['CountyName'] = value
            # elif tag == 'ProcessCode':
            #     raw_data['ProcessCode'] = value
            # elif tag == 'Parcel':
            #     raw_data['Parcel'] = value
            # elif tag == 'ParcelSeq':
            #     raw_data['ParcelSeq'] = value
            # elif tag == 'MapPageGrid':
            #     raw_data['MapPageGrid'] = value

        return online_data, raw_data

