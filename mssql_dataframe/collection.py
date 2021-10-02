"""Classes for all functionality within mssql_dataframe in a convenient package."""
import warnings

from mssql_dataframe import connect
from mssql_dataframe.core import custom_warnings, create, modify, read
from mssql_dataframe.core.write.write import write


class SQLServer:
    """Class containing methods for creating, modifying, reading, and writing between dataframes and SQL Server.

    If autoadjust_sql_objects is True SQL objects may be modified such as creating a table, adding a column,
    or increasing the size of a column. The exepection is internal tracking metadata columns _time_insert and
     _time_update which will always be created if include_metadata_timestamps=True.

    Parameters
    ----------
    connection (mssql_dataframe.connect) : connection for executing statements
    include_metadata_timestamps (bool, default=False) : include metadata timestamps _time_insert & _time_update in server time for write operations
    autoadjust_sql_objects (bool, default=False) : create and modify SQL table and columns as needed if True

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

    sql = SQLServer(db, autoadjust_sql_objects=True)
    """

    def __init__(
        self,
        connection: connect.connect,
        include_metadata_timestamps: bool = False,
        autoadjust_sql_objects: bool = False,
    ):

        # initialize mssql_dataframe functionality with shared connection
        self.connection = connection.connection
        self.create = create.create(connection)
        self.modify = modify.modify(connection)
        self.read = read.read(connection)
        self.write = write(
            connection, include_metadata_timestamps, autoadjust_sql_objects
        )

        # issue warnings for automated functionality
        if include_metadata_timestamps:
            warnings.warn(
                "SQL write operations will include metadata _time_insert & time_update columns as include_metadata_timestamps=True",
                custom_warnings.SQLObjectAdjustment,
            )

        if autoadjust_sql_objects:
            warnings.warn(
                "SQL objects will be created/modified as needed as autoadjust_sql_objects=True",
                custom_warnings.SQLObjectAdjustment,
            )
