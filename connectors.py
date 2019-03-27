import time
import math
import numpy as np
import pandas as pd
from itertools import islice
from urllib.parse import quote_plus
from sqlalchemy import create_engine, event


class Connector(object):
    """
    A class that works around the memory limitation that can be caused by inserting large pandas dataframes into a SQL storage.
    
    Example useage:
    
    import numpy as np
    import pandas as pd
    df = pd.DataFrame(np.random.random((100,10)))
    
    kwargs = {"db_type": 'postgresql',
        "address":'localhost',
        "user": "username",
        "password": "",
       "db_name": "testdb" }
       
       
    con = Connector(kwargs)
    con._init_engine() #If using other driver than pyodbc use con._init_engine(SET_FAST_EXECUTEMANY_SWITCH=False) 
    con.set_df('test_df', df)
    df = con.get_df('test_df')
    df = con.query_df('SELECT * FROM test_df') #For when running specific queries while using the same connection.

    
    """
    def __init__(self, engine_args, logger = None):
        self.logger = logger
        self.engine_args = engine_args
        self.connection_string = "{db_type}://{user}:{password}@{address}".format(**self.engine_args)
        if 'db_type' in self.engine_args.keys():
            self.connection_string += '/{db_name}'.format(**self.engine_args)
            if 'pyodbc' in self.engine_args['db_type']:
                conn = "DRIVER={driver};SERVER={address};DATABASE={db_name};UID={user};PWD={password}".format(**self.engine_args)
                self.connection_string = '{db_type}:///?odbc_connect={conn}'.format(**self.engine_args,
                                                                                   conn=quote_plus(conn))
                
    def _init_engine(self, SET_FAST_EXECUTEMANY_SWITCH=True):    
        self.engine = create_engine(self.connection_string, encoding='utf-8', echo=False)
        
        if SET_FAST_EXECUTEMANY_SWITCH:
            @event.listens_for(self.engine, 'before_cursor_execute')
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                if executemany:
                    cursor.fast_executemany = True
                
        if self.logger:  
            self.logger.info('initiated engine on address: {} with database: {} username: {}'.format(self.engine_args['address'],
                                                                                                     self.engine_args['db_name'],
                                                                                                     self.engine_args['user']))
                
    def _init_cursor(self):
        #Not in use
        conn = pymssql.connect(self.engine_args['address'], 
                              self.engine_args['user'],
                              self.engine_args['password'],
                              self.engine_args['db_name'])
        self.cursor = conn.cursor()
            
            
  
    def __write_df(self, table_name, df, **kwargs):
        df.to_sql(table_name, con = self.engine, **kwargs)
        return True
        
    def __write_split_df(self, table_name, dfs, **kwargs):
        self.__write_df(table_name, dfs[0], **kwargs)
        kwargs.pop('if_exists')
        for df in dfs[1:]:
            self.__write_df(table_name, df, if_exists = 'append', **kwargs)
        return True
        
    def __split_df(self, df, chunksize):
        chunk_count = int(math.ceil(df.size / chunksize))
        return np.array_split(df, chunk_count)
        
    def set_df(self, table_name, df, if_exists = 'replace', chunksize = 10**6, **kwargs):
        s = time.time() 
        status = False
        if chunksize is not None and df.size > chunksize:
            dfs = self.__split_df(df, chunksize)
            status = self.__write_split_df(table_name, dfs, if_exists = if_exists,  **kwargs)
        else:
            status = self.__write_df(table_name, df, if_exists = 'replace', **kwargs)
        if self.logger:
            self.logger.info('wrote name: {} dataframe shape: {} within: {}s'.format(table_name,
                                                                                     df.shape,
                                                                                     round(time.time()  - s, 4)))
        return status

    def get_df(self, table_name, chunk_count = None, **kwargs):
        s = time.time()
        if 'chunksize' not in kwargs.keys():
            kwargs['chunksize'] = 10**6
        dfs = pd.read_sql_table(table_name, self.engine, **kwargs)
        
        try:
            df = pd.concat(islice(dfs, chunk_count), axis = 0)
        except ValueError: #No objects to concetenate. dfs is a generator object so has no len() property!
            if self.logger:
                self.logger.warning("No objects to concetenate on table_name: {}".format(table_name))
            return None

        if self.logger:
            self.logger.info('fetched name: {} dataframe shape: {} within: {}'.format(table_name,
                                                                                      df.shape,
                                                                                      round(time.time()  - s, 4)))
        return df
      
    def get_table_names(self):
        if not hasattr(self, 'engine'):
            self._init_engine()
        df = pd.read_sql("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", self.engine)
        return df
    
    
    def query(self, _query, logging = False):
        if not hasattr(self, 'engine'):
            self._init_engine()
        connection = self.engine.connect()
        result = connection.execute(_query)
        if logging and self.logger:
            if self.logger:
                self.logger.info('ran query: {}'.format(_query))
        connection.close()
        return result
    
    def df_query(self, _query):
        if not hasattr(self, 'engine'):
            self._init_engine()
        result = pd.read_sql_query(_query, con = self.engine)
        return result

