import pyodbc

from mssql_dataframe.core import errors


class connect():
    """
    Connect to local, remote, or cloud SQL Server using ODBC connection.

    Parameters
    ----------

    database_name (str, default='master') : name of database to connect to
    server_name (str, default='localhost') : name of server to connect to
    driver (str, default=None) : ODBC driver name to use, if not given is automatically determined
    username (str, default=None) : if not given, use Windows account credentials to connect
    password (str, default=None) : if not given, use Windows account credentials to connect

    Properties
    ----------

    connection (pyodbc.Connection) : manage operations to database connection and database transactions

    Examples
    --------

    # local host connection using Windows account credentials and inferring the ODBC driver
    db = connect.connect()

    # remote server using username and password
    db = connect.connect(database_name='master', server_name='<remote>', username='<username>', password='<password>')

    # Azue SQL Server instance
    db = connect.connect(server_name='<server>.database.windows.net', username='<username>', password='<password>')

    # using a specific driver
    db = connect.connect(driver_name='ODBC Driver 17 for SQL Server')

    """


    def __init__(self, database_name: str = 'master', server_name: str = 'localhost',
        driver: str = None, username: str = None, password: str = None):

        driver = self._get_driver(driver)

        if username is None:
            self.connection = pyodbc.connect(
                driver=driver, server=server_name, database=database_name,
                autocommit=False, trusted_connection='yes'
            )
        else:
            self.connection = pyodbc.connect(
                driver=driver, server=server_name, database=database_name,
                autocommit=False, UID=username, PWD=password
            )

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
        if not driver:
            raise errors.EnvironmentODBCDriverNotFound('Unable to find ODBC driver.')
        driver = max(driver)

        return driver

