import pyodbc
import pandas as pd


class SQLServer():
    """
    Connect to SQL Server using ODBC connection.

    Parameters
    ----------

    database_name (str, default='master') : name of database to connect to

    server_name (str, default='localhost') : server to connect to

    driver (str, default=None) : if not given, find first driver "for SQL Server"

    fast_executemany (bool, default=True) : envoke pyodbc fast_execute mode

    autocommit (bool, default=True) : automatically commit transactions

    Returns
    -------

    connection (pyodbc.Connection)

    cursor (pyodbc.Cursor)

    """


    def __init__(self, database_name: str = 'master', server_name: str = 'localhost',
        driver: str = None, fast_executemany: bool = True, autocommit: bool = True):

        if driver is None:
            driver = self._get_driver()

        # TODO: connect using username and password
        self.connection = pyodbc.connect(
            driver=driver, server=server_name, database=database_name,
            autocommit=autocommit, trusted_connection='yes'
        )

        self.cursor = self.connection.cursor()
        self.cursor.fast_executemany = fast_executemany

        # self.sql2py, self.py2sql = self._type_mapping()

    
    def __repr__(self):
        return f"""SQLServer({self.database_name!r},{self.server_name!r},{self.driver},{self.fast_executemany},{self.autocommit})"""


    @staticmethod
    def _type_mapping():
        pass
        """
        Define relations between dataframe and SQL data types.

        Parameters
            None
        Returns
            sql2py          dict        keys = SQL types, values = Python types
            py2sql          dict        keys = Python types, values= SQL types
        """

        # Extract data types
        # mapping = vars(sqlalchemy.dialects.mssql)
        
        # mapping = {k:v for (k,v) in mapping.items() if callable(v) and k not in ['dialect','try_cast']}

        # # SQL to Python conversion
        # sql2py = {}
        # for k in mapping:
        #     try:
        #         sql2py[k] = mapping[k]().python_type
        #     except NotImplementedError:
        #         pass

        # # Python to SQL conversion
        # # # values are a list as different SQL types can map to a single Python type
        # py2sql = {k:[] for k in set(sql2py.values())}
        # for (k,v) in sql2py.items():
        #     py2sql[v].append(k)

        # return sql2py, py2sql


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
            raise ODBCDriverNotFound('Unable to automatically determine ODBC driver.')
        driver = driver[0]

        return driver


class AzureSQLDatabase():
    """
    TODO: test AzureSQLDatabase and add to contributing documentation (free tier needed) 

    Returns
    -------

    connection (?)

    cursor (?)

    """    

    def __init__(self):

        self.connection = None
        self.cursor = None

        raise NotImplementedError('AzureSQLDatabase not yet implemented')


class ODBCDriverNotFound(Exception):
    '''Exception for not automatically determining ODBC driver.'''
    pass