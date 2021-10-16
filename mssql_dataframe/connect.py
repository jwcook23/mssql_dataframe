"""Class for establishing connection to an SQL database."""

import pyodbc

from mssql_dataframe.core import custom_errors


class connect:
    """
    Connect to local, remote, or cloud SQL Server using ODBC connection.

    Parameters
    ----------

    database (str, default='master') : name of database to connect to
    server (str, default='localhost') : name of server to connect to
    driver (str, default=None) : ODBC driver name to use, if not given is automatically determined
    username (str, default=None) : if not given, use Windows account credentials to connect
    password (str, default=None) : if not given, use Windows account credentials to connect

    Properties
    ----------

    connection (pyodbc.Connection) : manage operations to database connection and database transactions

    Examples
    --------

    #### local host connection using Windows account credentials and inferring the ODBC driver
    db = connect()

    #### remote server using username and password
    db = connect(database='master', server='<remote>', username='<username>', password='<password>')

    #### Azure SQL Server instance
    db = connect(server='<server>.database.windows.net', username='<username>', password='<password>')

    #### using a specific driver
    db = connect(driver_name='ODBC Driver 17 for SQL Server')

    """

    def __init__(
        self,
        database: str = "master",
        server: str = "localhost",
        driver: str = None,
        username: str = None,
        password: str = None,
    ):

        driver = self._get_driver(driver)

        if username is None:
            self.connection = pyodbc.connect(
                driver=driver,
                server=server,
                database=database,
                autocommit=False,
                trusted_connection="yes",
            )
        else:
            self.connection = pyodbc.connect(
                driver=driver,
                server=server,
                database=database,
                autocommit=False,
                UID=username,
                PWD=password,
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
        installed = [x for x in pyodbc.drivers() if x.endswith(" for SQL Server")]
        if driver_search is None:
            driver = [x for x in installed if x.endswith(" for SQL Server")]
        else:
            driver = [x for x in installed if x == driver_search]
        if not driver:
            raise custom_errors.EnvironmentODBCDriverNotFound(
                "Unable to find ODBC driver."
            )
        driver = max(driver)

        return driver
