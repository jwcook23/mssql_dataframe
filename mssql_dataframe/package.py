"""Methods for creating, modifying, reading, and writing between dataframes and SQL."""
from importlib.metadata import version
import sys
import logging

from mssql_dataframe.connect import connect
from mssql_dataframe.core import (
    custom_errors,
    conversion,
    create,
    modify,
    read,
)
from mssql_dataframe.core.write.write import write

logger = logging.getLogger(__name__)


class SQLServer(connect):
    """Class containing methods for creating, modifying, reading, and writing between dataframes and SQL Server.

    Parameters
    ----------
    database (str, default='master') : name of database to connect to
    server (str, default='localhost') : name of server to connect to
    driver (str, default=None) : ODBC driver name to use, if not given is automatically determined
    username (str, default=None) : if not given, use Windows account credentials to connect
    password (str, default=None) : if not given, use Windows account credentials to connect
    include_metadata_timestamps (bool, default=False) : include metadata timestamps _time_insert & _time_update in server time for write operations

    Properties
    ----------
    create : methods for creating SQL tables objects
    modify : methods for modifying tables columns and primary keys
    read : methods for reading from SQL tables
    write : methods for inserting, updating, and merging records

    Examples
    --------
    Connect to localhost server master database.

    >>> import env
    >>> sql = SQLServer(database=env.database, server=env.server)

    Enable logging from mssql_dataframe.

    >>> import logging
    >>> logging.basicConfig(
    ... filename='example.log', encoding='utf-8', level=logging.DEBUG,
    ... format='%(asctime)s %(name)s %(filename)s %(levelname)s: %(message)s'
    ... )
    >>> logger = logging.getLogger('mssql_dataframe')
    >>> sql = SQLServer(database=env.database, server=env.server)

    See Also
    --------
    connect : Additional options for connecting to a server including remote, Azure, and username/password.
    """

    def __init__(
        self,
        include_metadata_timestamps: bool = False,
        **kwargs
    ):
        connect.__init__(self, **kwargs)

        # log initialization details
        self.log_init()

        # initialize mssql_dataframe functionality with shared connection
        self.exceptions = custom_errors
        self.create = create.create(self.connection, include_metadata_timestamps)
        self.modify = modify.modify(self.connection)
        self.read = read.read(self.connection)
        self.write = write(self.connection, include_metadata_timestamps)

        # issue warnings for automated functionality
        if include_metadata_timestamps:
            msg = "SQL write operations will include metadata '_time_insert' & '_time_update' columns as 'include_metadata_timestamps=True'."
            logger.warning(msg)

    def log_init(self):
        """Log connection info and versions for Python, SQL, and required packages."""
        # determine versions for debugging
        self.version_spec = {}
        # Python
        self.version_spec["python"] = sys.version_info
        # SQL
        cur = self.connection.cursor()
        name = cur.execute("SELECT @@VERSION").fetchone()
        self.version_spec["sql"] = name[0]
        # packages
        names = ["mssql-dataframe", "pyodbc", "pandas"]
        for name in names:
            self.version_spec[name] = version(name)

        # output actual connection info (possibly derived within connection object)
        # logger.debug(f"Connection Info: {self.connection_spec}")
        # output Python/SQL/package versions
        logger.debug(f"Version Numbers: {self.version_spec}")

    def get_schema(self, table_name: str):
        """Get schema of an SQL table and the defined conversion rules between data types.

        Parameters
        ----------
        table_name (str) : table name to read schema from

        Returns
        -------
        schema (pandas.DataFrame) : table column specifications and conversion rules
        """
        schema, _ = conversion.get_schema(self.connection, table_name)

        return schema
