import sqlalchemy as sqlalchemy
import pyodbc as db
import pandas as pd
from configs import EnvironmentConfig
from utils import get_logger
from utils.SqlUtils import SqlUtils
import inspect
import math

# log = get_logger('Database')

env_obj = EnvironmentConfig()

class Database(object):
    def __init__(self, conn_type, server, db_name, logger , app_name:str = "DTSPython", user=None, pw=None):
        self.log = logger        
        self.log.info("conn_type="+conn_type+",server="+server+",db_name="+db_name+",app_name="+app_name)   
        db.pooling = False
        db
        if conn_type == 'sqlserver':
            try:
                driver_port = env_obj.get_sql_driver()                
                if user is None and pw is None:
                    self.con = db.connect(
                        f'DRIVER={driver_port};Server={server};Database={db_name};Trusted_connection=yes; Application Name={app_name}')
                else:
                    self.con = db.connect(
                        f'DRIVER={driver_port};Server={server};Database={db_name};UID={user};PWD={pw}; Application Name={app_name}')
                self.cursor = self.con.cursor()                        
            except Exception as e:
                self.log.exception(e, exc_info=e)
                raise
                # exit(1)
        elif conn_type == 'sqlalchemy':
            try:
                driver_port = env_obj.get_sql_driver().replace('{','').replace('}','').replace(' ','+')
                if user is None and pw is None:
                    conn_string = f'mssql+pyodbc://{server}/{db_name}?driver={driver_port}' # ?Trusted_connection=yes' <-- This is causing issues??? Anton
                else:
                    conn_string = f'mssql+pyodbc://{server}/{db_name}?driver={driver_port}?UID={user};PWD={pw}'
                self.log.info("conn_string="+conn_string)
                # self.engine = sqlalchemy.create_engine(conn_string, connect_args={"timeout": 30, "app": "DTS " + app_name})
                self.engine = sqlalchemy.create_engine(conn_string, fast_executemany=True, connect_args={"timeout": 30, "app": "DTS " + app_name})
                if self.engine is None:
                    error = f'No sqlalchemy connection to {server} {db_name}'
                    self.log.exception(error)
                    raise ValueError(error) 
                else:
                    self.con = self.engine.raw_connection()
                    self.cursor = self.con.cursor()
            except Exception as e:
                self.log.exception(e, exc_info=e)
                raise
                # exit(1)                

        #elif conn_type == 'mysql':
        #    try:
        #        self.con = sql.connect(host=server, user=user, passwd=pw, db=db_name, port=int(driver_port))
        #    except Exception as e:
        #        self.log.exception(e, exc_info=e)
        if self.con is None:
            error = f'No connection to {server} {db_name}'
            self.log.exception(error)
            raise ValueError(error) 
            # exit(1)

    @classmethod
    def connect_diablo(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_diablo_server()
        db_name = env_obj.get_diablo_db()
        log.info("conn_type="+conn_type+",server="+server+",db_name="+db_name)
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_diablo_fulfillment(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_diablo_fulfillment_server()
        db_name = env_obj.get_diablosynonyms_db()
        log.info("conn_type="+conn_type+",server="+server+",db_name="+db_name)
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_autoreports(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_autoreports_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_pvcstracker(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_pvcstracker_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_readme(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_readme_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_import(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_import_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)
        
    @classmethod
    def connect_dts_dqweb(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_dqweb_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_antifraud(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_antifraud_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_eagle(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        # server = env_obj.get_dts_server()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_eagle_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)
    
    @classmethod
    def connect_dts_adc(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_adc_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_global(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_global_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_qcpro(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        # server = env_obj.get_dts_server()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_qcpro_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    @classmethod
    def connect_dts_dataquick(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_dataquick_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)
    
    @classmethod
    def connect_dts_inventorytracker2(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_inventorytracker2_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)
    
    @classmethod
    def connect_dts_documentcounts(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_documentcounts_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)
    
    @classmethod
    def connect_dts_countyapnprofile(cls, conn_type: str='', app_name:str = "DTSPython", log = None):
        if log is None:
            log = get_logger('Database')
        log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        if conn_type == '':
            conn_type = env_obj.get_edg_connection()
        server = env_obj.get_dts_server()
        db_name = env_obj.get_dts_countyapnprofile_db()
        return cls(conn_type=conn_type, server=server, db_name=db_name, app_name=app_name, logger=log)

    def fn_execute(self, query, commit: bool = True, close_conn: bool = True):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " query=" + query)
        try:
            self.cursor.execute(query)
            if commit:
                self.cursor.commit()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise
            # exit(1)
        else:
            if close_conn:
                self.cursor.close()
                del self.cursor
                self.con.close()
    
    def fn_fetch(self, query, fetch: str = 'fetchall', commit: bool = False, close_conn: bool = True, get_next_buffer: bool = False, buffer_size: int = 10, return_dataframe: bool = False):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + " query=" + query)
        data = ''
        try:
            if not get_next_buffer: # In a fetchmany execution, only the first call should execute the actual query
                self.cursor.execute(query)
            if fetch == 'fetchall':
                data = self.cursor.fetchall()
            elif fetch == 'fetchval':
                data = self.cursor.fetchval()
            elif fetch == 'fetchone':
                data = self.cursor.fetchone()
            elif fetch == 'fetchmany':
                data = self.cursor.fetchmany(buffer_size)
            if commit:
                self.cursor.commit()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise
            # exit(1)
        else:
            if return_dataframe:
                df = pd.DataFrame.from_records(data, columns=[col[0] for col in self.cursor.description])            
                data = df
            if close_conn:
                self.cursor.close()
                del self.cursor
                self.con.close()
        return data

    def fn_executemany(self, query, data, commit: bool = False, close_conn: bool = True):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", query=" + query)
        try:
            self.cursor.fast_executemany = True
            self.cursor.executemany(query, data)
            if commit:      
                self.cursor.commit()
            if close_conn:
                self.cursor.close()
                del self.cursor
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise
            # exit(1)
        return

    def fn_close(self):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        self.cursor.close()
        del self.cursor
        self.con.close()
        return

    def fn_populate_dataframe(self,query, cnvrt_to_none: bool = False):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function) + ", query=" + query)
        self.log.info(self.engine)
        result = pd.read_sql(query, self.con)
        if cnvrt_to_none:
            '''convert nan to NaN (None) in order to be able to insert NULLs'''
            result = result.astype(object).where(pd.notnull(result), None)            
        self.log.info("Number of rows in dataframe = " + str(len(result.index)))
        return result

    def fn_load_dataframe_to_table(self, dest_table_name, dataframe, truncate_table: bool = False, commit: bool = True, close_conn: bool = True, cnvrt_to_none: bool = False):
        self.log.info('fnc: {}'.format(inspect.getframeinfo(inspect.currentframe()).function))
        try:
            if truncate_table:
                self.fn_execute("truncate table " + dest_table_name, commit = False, close_conn = False)
            query = SqlUtils.InsertStmt(dest_table_name, dataframe)
            if cnvrt_to_none:
                '''convert nan to NaN (None) in order to be able to insert NULLs'''
                dataframe = dataframe.astype(object).where(pd.notnull(dataframe), None)            
            self.fn_executemany(query, list(dataframe.itertuples(index=False, name=None)), commit, close_conn)
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise   

    # def fn_insert_with_progress(self, df, engine, table="", schema=""):
    def fn_to_sql(self, dest_schema: str = 'dbo', dest_table_name: str = None, dataframe = None, chunk_limit: int=100000, truncate_table: bool = False, commit: bool = True, close_conn: bool = True):
        try:
            if truncate_table:
                self.fn_execute(f'truncate table {dest_schema}.{dest_table_name}', commit = True, close_conn = False)

            chunksize = math.floor(chunk_limit / len(dataframe.columns))

            for chunk in chunker(dataframe, chunksize):
                chunk.to_sql(
                    name=dest_table_name,
                    schema=dest_schema,
                    con=self.engine,
                    if_exists="append",
                    index=False
                )
            if commit:      
                self.cursor.commit()
            if close_conn:
                self.cursor.close()
                del self.cursor
                self.con.close()
        except Exception as e:
            self.log.exception(e, exc_info=e)
            raise   

def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))
