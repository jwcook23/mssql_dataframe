"""Classes for all functionality within mssql_dataframe in a convenient package."""
import warnings

from mssql_dataframe import connect
from mssql_dataframe.core import create, errors, modify, read
from mssql_dataframe.core.write.write import write


class SQLServer:
    """Class containing methods for creating, modifying, reading, and writing between dataframes and SQL Server.

    If auto_adjust_sql_objects is True, SQL objects may be modified. The exepection is internal tracking columns
    _time_insert and _time_update which will always be created if include_timestamps=True for write methods.

    Parameters
    ----------
    connection (mssql_dataframe.connect) : connection for executing statements
    auto_adjust_sql_objects (bool, default=False) : create and modify SQL table and columns as needed if True

    Properties
    ----------
    create : methods for creating SQL tables objects
    modify : methods for modifying tables columns and primary keys
    read : methods for reading from SQL tables
    write : methods for inserting, updating, and merging records

    Example
    -------

    #### connect to a local host database, with the ability to automatically adjust SQL objects
    db = connect()

    sql = SQLServer(db, auto_adjust_sql_objects=True)


    """

    def __init__(
        self, connection: connect.connect, auto_adjust_sql_objects: bool = False
    ):

        # initialize mssql_dataframe functionality with shared connection
        self.connection = connection.connection
        self.create = create.create(connection)
        self.modify = modify.modify(connection)
        self.read = read.read(connection)
        self.write = write(connection, auto_adjust_sql_objects)

        if auto_adjust_sql_objects:
            warnings.warn(
                "SQL objects will be created/modified as needed as auto_adjust_sql_objects=True",
                errors.SQLObjectAdjustment,
            )
