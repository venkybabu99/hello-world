# To Document wsdl, from command line execute: python -mzeep http://addressplus.corelogic.com:14100/addressplus/addrplus.wsdl
import asyncio
import time
from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport
from zeep import helpers


class Dict2Class(object):
# Turns a dictionary into a class
#################################
    def __init__(self, my_dict):
          
        for key in my_dict:
            setattr(self, key, my_dict[key])
  
def StandardizeAddress(Address,City,State='',ZIPCode='',AddressType='s',Country='',ForeignLastLine=''):
# Uses the interactive version of AddrPlus to standardize an address
# Returns a dictionary that contains key value pairs of the standardized address
######################################################################################################
    try:
        wsdl='http://addressplus.corelogic.com:14100/addressplus/addrplus.wsdl'
        transport = Transport(timeout=10,cache=SqliteCache())
        settings = Settings(strict=False) #,raw_response=True)
        # client = Client(wsdl, settings=settings, transport=transport,service_name='AddressPlusService',port_name='AddressPlusPort')
        client = Client(wsdl, settings=settings, transport=transport,service_name='AddressPlusService',port_name='AddressPlusPort')
        response = client.service.CLStandardizeAddress(
            Version='1.0',
            User='cmasuser',
            Password='cmadr8in',
            Config= 'CMAS1',
            Address= Address.replace('#',''),
            City= City.replace('#',''),
            State= State.replace('#',''),
            ZIPCode= ZIPCode.replace('#',''),
            Country= Country,
            ForeignLastLine= ForeignLastLine,
            AddressType = AddressType
        )
        addr = helpers.serialize_object(response)
        #print(pyl)
        #print(pyl.keys())
        #print(response)
        del addr['_raw_elements']
        return addr # return key value pairs as dictionary object

    except Exception as inst:
        print("*** Error in function StandardizeAddress ***")
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst)          # __str__ allows args to be printed directly,
                             # but may be overridden in exception subclasses
        raise(inst)

#async def main():
#    addr = await StandardizeAddress(Address='4461 Brintnall Street', City='Port Charlotte',State='FL',AddressType='s',ZIPCode='33948')
#    for i in addr.keys():
#        print(i+'='+str(addr.get(i)))

#asyncio.run(main())

print(time.strftime("%H:%M:%S", time.localtime()))
for i in range(1000):
    addr = StandardizeAddress(Address='4461 Brintnall Street', City='Port Charlotte',State='FL',AddressType='s',ZIPCode='33948')
print(time.strftime("%H:%M:%S", time.localtime()))
#for i in addr.keys():
#    print(i+'='+str(addr.get(i)))
#result = Dict2Class(addr)
#for attr in dir(result):
#    print(attr)