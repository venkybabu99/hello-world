# from configs.ConfigUtility import AppConfig
# from utils import get_logger
# from configs import EnvironmentConfig
# env_prop = EnvironmentConfig()

# log = get_logger('SqlUtils')

class SqlUtils():
    # def __init__(self):
    #     self.app_obj = AppConfig()

    @staticmethod
    def InsertStmt(desttable: str, df: dict) -> str:
        cols = df.columns
        query = "INSERT INTO {} ( ".format(desttable)
        insvalues = " VALUES ( "
        for i in range(len(cols)):
            if i > 0:
                prfx = ", "
            else:
                prfx = ""
            query += prfx + "[" + str(cols[i]) + "]"
            insvalues += prfx + "?"
        query += " )" + insvalues + " )"
        return query

