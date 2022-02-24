"""Class for establishing connection to an SQL database."""

import pyodbc

from mssql_dataframe.core import custom_errors


class connect:
    r"""Connect to local, remote, or cloud SQL Server using ODBC connection.

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
    Connect to local host server master database using Windows account credentials and inferring the ODBC driver.

    >>> db = connect()
    >>> db.connection_spec['server'] == 'localhost'
    True

    Connect to a remote server.

    >>> db = connect(server='SomeServerName', database='tempdb') # doctest: +SKIP

    Connect to Azure SQL Server instance using a username and password.

    >>> db = connect(server='<server>.database.windows.net', username='<username>', password='<password>') # doctest: +SKIP

    Connect to SQL Express Local DB
    >>> db = connect(server=r"(localdb)\mssqllocaldb") # doctest: +SKIP

    Connect using a specific driver.
    >>> db = connect(driver_name='ODBC Driver 17 for SQL Server') # doctest: +SKIP
    """

    def __init__(
        self,
        database: str = "master",
        server: str = "localhost",
        driver: str = None,
        username: str = None,
        password: str = None,
    ):

        driver, drivers_installed = self._get_driver(driver)
        self.connection_spec = {
            "database": database,
            "server": server,
            "driver": driver,
            "drivers_installed": drivers_installed,
        }
        if username is None:
            self.connection_spec["trusted_connection"] = True
        else:
            self.connection_spec["trusted_connection"] = False

        if self.connection_spec["trusted_connection"]:
            self.connection = pyodbc.connect(
                driver=self.connection_spec["driver"],
                server=self.connection_spec["server"],
                database=self.connection_spec["database"],
                autocommit=False,
                trusted_connection="yes",
            )
        else:
            self.connection = pyodbc.connect(
                driver=self.connection_spec["driver"],
                server=self.connection_spec["server"],
                database=self.connection_spec["database"],
                autocommit=False,
                UID=username,
                PWD=password,
            )

    @staticmethod
    def _get_driver(driver_search):
        """Automatically determine ODBC driver if needed.

        Parameters
        ----------
        driver_search (str) : name of ODBC driver, if None, automatically determine

        Returns
        -------
        driver (str) : name of ODBC driver
        drivers_installed (list) : drivers install for SQL Server
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

        return driver, installed
