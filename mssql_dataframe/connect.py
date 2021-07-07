import pyodbc

import mssql_dataframe.errors


class connect():
    """
    Connect to local, remote, or cloud SQL Server using ODBC connection.

    Parameters
    ----------

    database_name (str, default='master') : name of database to connect to
    server_name (str, default='localhost') : name of server to connect to
    driver (str, default=None) : ODBC driver name to use, if not given is automatically determined
    fast_executemany (bool, default=True) : increases performance of executemany operations
    autocommit (bool, default=True) : automatically commit transactions
    username (str, default=None) : if not given, use Windows account credentials to connect
    password (str, default=None) : if not given, use Windows account credentials to connect

    Properties
    ----------

    connection (pyodbc.Connection) : manage operations to database connection and database transactions
    cursor (pyodbc.Cursor) : database cursor to manage read and write operations

    Examples
    --------

    #### local host connection using Windows account credentials and inferring the ODBC driver
    db = connect.connect()

    #### remote server using username and password
    db = connect.connect(database_name='master', server_name='<remote>', username='<username>', password='<password>')

    #### Azue SQL Server instance
    db = connect.connect(server_name='<server>.database.windows.net', username='<username>', password='<password>')

    #### using a specific driver
    db = connect.connect(driver_name='ODBC Driver 17 for SQL Server')

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

