import pyodbc
import sqlalchemy as db
import pandas as pd

import sqlalchemy.dialects.mssql

from . import exceptions


class create():

    def __init__(self):
        pass


    def table(self, table_name, dataframe, if_exists: str='error'):
        """
        Create database table using a dataframe.

        Parameters
            table_name      str                     name of table
            dataframe       DataFrame               dataframe to determine datatypes of columns
            if_exists       str, default='error'    if table already exists: 'error', 'ignore', 'drop'

        Returns
            None
        """

        # TODO: primary key and autoincrementing behavior
        # # sql.Column('RowID', db.Integer, primary_key=True, autoincrement=False)

        # Convert dataframe types to standard Python types
        columns = dataframe.columns

        dtypes = {}
        for c in columns:
            try:
                dtypes[c] = type(dataframe.loc[0,c].item())     # numpy type
            except:
                dtypes[c] = type(dataframe.loc[0,c])            # core Python type

        # test = sqlalchemy.dialects.mssql.TINYINT()
        # test.compare_values(1,2,1)

        # self.py2sql[dtypes['Address']]
        # <class 'str'>: ['CHAR', 'NCHAR', 'NTEXT', 'NVARCHAR', 'TEXT', 'VARCHAR', 'XML']

    
    def dataframe(self):
        pass
        # sqlalchemy.dialects.mssql.INTEGER.result_processor


class SQLServer(create):
    """
    Connect to SQL Server.

    Parameters

        database_name       str                         name of database
        server_name         str, default='localhost'    server to connect with
        driver              str, default=None           if not given, find first driver "for SQL Server"
        fast_executemany    bool, default=True          envoke pyodbc fast_execute mode
        autocommit          bool, default=True          automatically commit transactions

    Returns

        self.engine         sqlalchemy engine
    """


    def __init__(self, database_name: str, server_name: str = 'localhost',
        driver: str = None, fast_executemany: bool = True, autocommit: bool = True):

        self.database_name = database_name
        self.server_name = server_name
        self.driver = driver
        self.fast_executemany = fast_executemany
        self.autocommit = autocommit

        if self.driver is None:
            self.driver = self._get_driver()

        # TODO: connect using username and password
        url = 'mssql+pyodbc://@'+server_name+'/'+database_name+'?&driver='+self.driver+'&autocommit='+str(self.autocommit)

        self.engine = db.create_engine(url, fast_executemany=self.fast_executemany)

        self.metadata = db.MetaData(bind=True)

        self.sql2py, self.py2sql = self._type_mapping()

    
    def __repr__(self):
        return f"""SQLServer({self.database_name!r},{self.server_name!r},{self.driver},{self.fast_executemany},{self.autocommit})"""

    @staticmethod
    def _type_mapping():
        """
        Define relations between dataframe and SQL data types.

        Parameters
            None
        Returns
            sql2py          dict        keys = SQL types, values = Python types
            py2sql          dict        keys = Python types, values= SQL types
        """

        # Extract data types
        mapping = vars(sqlalchemy.dialects.mssql)
        
        mapping = {k:v for (k,v) in mapping.items() if callable(v) and k not in ['dialect','try_cast']}

        # SQL to Python conversion
        sql2py = {}
        for k in mapping:
            try:
                sql2py[k] = mapping[k]().python_type
            except NotImplementedError:
                pass

        # Python to SQL conversion
        # # values are a list as different SQL types can map to a single Python type
        py2sql = {k:[] for k in set(sql2py.values())}
        for (k,v) in sql2py.items():
            py2sql[v].append(k)

        return sql2py, py2sql


    @staticmethod
    def _get_driver():
        """
        Automatically determine ODBC driver if needed.

        Parameters
            None
        Returns
            driver      str         name of ODBC driver
        """
        driver = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]
        if len(driver)==0:
            raise exceptions.ODBCDriverNotFound('Unable to automatically determine ODBC driver.')
        driver = driver[0]

        return driver