import pyodbc

import mssql_dataframe.errors


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
        driver: str = None, fast_executemany: bool = True, autocommit: bool = True,
        username: str = None, password: str = ''):

        driver = self._get_driver(driver)

        if username is None:
            self.connection = pyodbc.connect(
                driver=driver, server=server_name, database=database_name,
                autocommit=autocommit, trusted_connection='yes'
            )
        else:
            self.connection = pyodbc.connect(
                driver=driver, server=server_name, database=database_name,
                autocommit=autocommit, UID=username, PWD=password
            )

        self.cursor = self.connection.cursor()
        self.cursor.fast_executemany = fast_executemany


    @staticmethod
    def _get_driver(driver_search):
        """
        Automatically determine ODBC driver if needed.

        Parameters
        ----------
        
        driver_search (str) : name of ODBC driver, if None, automatically determine

        Returns
        -------

        driver (str) : name of ODBC driver
        """
        installed = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]
        if driver_search is None:
            driver = [x for x in installed if x.endswith(' for SQL Server')]
        else: 
            driver = [x for x in installed if x==driver_search]
        if len(driver)!=1:
            raise mssql_dataframe.errors.ODBCDriverNotFound('Unable to find ODBC driver.') from None
        driver = driver[0]

        return driver


class AzureSQL():
    """

    Parameters
    ---------- 

    Returns
    -------

    connection (?)
    cursor (?)

    """    

    def __init__(self):
        # TODO: define AzureSQL class
        raise NotImplementedError('AzureSQL not yet implemented') from None

