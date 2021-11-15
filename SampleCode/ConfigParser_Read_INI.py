import configparser
config = configparser.ConfigParser()
#print(config.sections())
config.read('example.ini')
#print(config.sections())
#print('bitbucket.org' in config)
#print('bytebong.com' in config)
#print(config['bitbucket.org']['User'])
#print(config['DEFAULT']['Compression'])
#print(config['DEFAULT']['compressionlevel'])
#topsecret = config['topsecret.server.com']
#print(topsecret['ForwardX11'])
#print(topsecret['Port'])
for key in config['bitbucket.org']: 
    print(key)
#print(config['bitbucket.org']['ForwardX11'])