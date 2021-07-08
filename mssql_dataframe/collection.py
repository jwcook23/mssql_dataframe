from mssql_dataframe import connect
from mssql_dataframe.core import create, modify, read, write

class SQLServer():
    """
    Class containing methods for creating, modifying, reading, and writing between dataframes and SQL Server.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statements

    Properties
    ----------

    create (mssql_dataframe.connect.connect) : methods for creating SQL tables objects
    modify (mssql_dataframe.modify.modify) : methods for modifying tables columns
    read (mssql_dataframe.read.read) : methods for reading from SQL tables
    write (mssql_dataframe.write.write) : methods for inserting, updating, and merge records


    Examples
    --------


    """


    def __init__(self, connection: connect.connect):
    
        # initialize mssql_dataframe functionality with shared connection
        self.create = create.create(connection)
        self.modify = modify.modify(connection)
        self.read = read.read(connection)
        self.write = write.write(connection)
