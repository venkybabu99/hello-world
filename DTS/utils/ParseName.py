# from utils import get_logger
# from configs import EnvironmentConfig

# env_prop = EnvironmentConfig()

# log = get_logger('ParseName')

def ParseName(corplist, rawname):

    tnames = []
    tnames_pb = []
    resultnames = []
    j = 0
    lastname = None
    firstname, middlename, suffixname = NameClear()
    nameparsed = rawname.split()
    for i, namepart in enumerate(nameparsed):
        ''' Check for Corp Tag '''
        findCorptag = FindCorp(corplist, namepart)
        if findCorptag:
            tnames = []
            tnames_pb = []
            tnames.append(rawname)
            lastname = None
            break
        if namepart == '&' or namepart == 'AND':
            if not ( lastname is None and firstname is None and middlename is None and suffixname is None ):
                ''' c# push_back '''
                tnames_pb.append(NameMerge( lastname, firstname, middlename, suffixname) )
                firstname, middlename, suffixname = NameClear()
                j = 1
        elif lastname is None and j == 0:
            lastname = namepart
            j = j + 1
        elif firstname is None and j == 1:
            firstname = namepart
            j = j + 1
        elif middlename is None and len(namepart) == 1 and j == 2:
            middlename = namepart
            j = j + 1
        elif suffixname is None and j == 3:
            suffixname = namepart
            j = j + 1
        else:
            ''' Must be a Corporate '''
            tnames = []
            tnames_pb = []
            tnames.append(rawname)
            lastname = None
            break

    ''' End of Name loop '''
    fullname = NameMerge( lastname, firstname, middlename, suffixname )
    if fullname is not None:
        tnames.append(fullname)

    for tname in tnames_pb:
        resultnames.append(tname)

    for tname in tnames:
        resultnames.append(tname)

    return resultnames


def FindCorp(corplist, parsename):
    for row in corplist.itertuples(index=False):
        if row[0] == parsename:
            return True
    return False


def NameMerge(lastname, firstname, middlename, suffixname):
    fullname = None
    if lastname is not None:
        fullname = lastname
        if firstname is not None and firstname > '':
            fullname += " " + firstname
            if middlename is not None and middlename > '':
                fullname += " " + middlename
                if suffixname is not None and suffixname > '':
                    fullname += " " + suffixname
    return fullname


def NameClear():
    firstname = None
    middlename = None
    suffixname = None
    return firstname, middlename, suffixname
