import warnings

from mssql_dataframe import connect
from mssql_dataframe.core import create, errors, modify, read
from mssql_dataframe.core.write import insert, update, merge

class SQLServer():
    """
    Class containing methods for creating, modifying, reading, and writing between dataframes and SQL Server.

    If adjust_sql_objects is True, SQL objects may be modified. The exepection is internal tracking columns 
    _time_insert and _time_update which will always be created if include_timestamps=True for write methods.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statements
    adjust_sql_objects (bool, default=False) : create and modify SQL table and columns as needed if True
    adjust_sql_attempts (int, default=10) : maximum attempts at adjusting_sql_objects after write failure

    Properties
    ----------

    create (mssql_dataframe.connect.connect) : methods for creating SQL tables objects
    modify (mssql_dataframe.modify.modify) : methods for modifying tables columns
    read (mssql_dataframe.read.read) : methods for reading from SQL tables
    write (mssql_dataframe.write.write) : methods for inserting, updating, and merge records

    """


    def __init__(self, connection: connect.connect, adjust_sql_objects: bool = False):
    
        # initialize mssql_dataframe functionality with shared connection
        self.connection = connection
        self.create = create.create(connection)
        self.modify = modify.modify(connection)
        self.read = read.read(connection)
        self.insert = insert.insert(connection)
        self.update = update.update(connection)
        self.merge = merge.merge(connection)

        if adjust_sql_objects:
            warnings.warn("SQL objects will be created/modified as needed as adjust_sql_objects=True", errors.SQLObjectAdjustment)
