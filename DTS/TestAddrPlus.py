from utils.AddressPlus import AddressPlus
raw_sit_addr = '00134 ALAMEDA AVE N'
raw_sit_city = 'AZUSA'
raw_sit_st = 'CA'
raw_sit_zip = '917023603'
siteonln, siteraw = AddressPlus.addrplus(fulladdress=raw_sit_addr,city=raw_sit_city,state=raw_sit_st,zipcode=raw_sit_zip,country='',foreignlastline='',addresstype='S')
# print(siteraw)
print(siteraw['Latitude'])
latitude_split = siteraw['Latitude'].split('.')
lat = f'{latitude_split[0]}.{latitude_split[1][:6]}'
print(str(float(siteraw['Latitude'])))
print(lat)
# raw_sit_addr = '00110 GUADALUPE AVE S'
# raw_sit_city = 'REDONDO BEACH'
# raw_sit_st = 'CA'
# raw_sit_zip = '902773460'
# siteonln, siteraw = AddressPlus.addrplus(fulladdress=raw_sit_addr,city=raw_sit_city,state=raw_sit_st,zipcode=raw_sit_zip,country='',foreignlastline='',addresstype='M')
# print(siteraw)
# print(siteraw['Latitude'])
# print(str(float(siteraw['Latitude'])))
print('done')