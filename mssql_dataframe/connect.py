"""Class for establishing connection to an SQL database."""

import pyodbc

from mssql_dataframe.core import custom_errors


class connect:
    r"""Connect to local, remote, or cloud SQL Server using ODBC connection.

    kwargs are passed directly to pyodbc.connect as keyword arguments
    - https://github.com/mkleehammer/pyodbc/wiki/The-pyodbc-Module#connect
    - see pyodbc.connect for more documentation and the full set of parameters
    - autocommit is set to always be False as the commit is handled by mssql_dataframe
    - if a driver is not provided, it is inferred using pyodbc

    Parameters
    ----------
    keyword database (str) : name of database to connect to
    keyword server (str') : name of server to connect to
    keyword driver (str) : ODBC driver name to use, if not given is automatically determined
    keyword UID (str) : if not given, use Windows account credentials to connect
    keyword PWD (str) : if not given, use Windows account credentials to connect

    Properties
    ----------
    connection (pyodbc.Connection) : manage operations to database connection and database transactions

    Examples
    --------
    Connect using Windows account credentials and inferring the ODBC driver.

    >>> import env
    >>> db = connect(server=env.server, database=env.database)

    Connect to a remote server.

    >>> db = connect(server='SomeServerName', database='tempdb') # doctest: +SKIP

    Connect to Azure SQL Server instance using a username and password.

    >>> db = connect(server='<server>.database.windows.net', UID'<username>', PWD='<password>') # doctest: +SKIP

    Connect to SQL Express Local DB
    >>> db = connect(server=r"(localdb)\mssqllocaldb") # doctest: +SKIP

    Connect using a specific driver.
    >>> db = connect(driver_name='ODBC Driver 17 for SQL Server') # doctest: +SKIP

    Connect using any ODBC connection parameter.

    >>> db = connect(server='SomeServerName', database='tempdb', TrustServerCertificate='yes') # doctest: +SKIP
    """

    def __init__(self, **kwargs):
        kwargs["autocommit"] = False

        if "driver" not in kwargs or kwargs["driver"] is None:
            driver, _ = self._get_driver(None)
            kwargs["driver"] = driver
        else:
            _ = self._get_driver(kwargs["driver"])

        self.connection = pyodbc.connect(**kwargs)

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
