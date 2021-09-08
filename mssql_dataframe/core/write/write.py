from mssql_dataframe.core import create, modify
from insert import insert

class write(insert):

    def __init__(self, connection, adjust_sql_objects: bool = False):
        '''Class for inserting, updating, and merging into an SQL table.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        adjust_sql_objects (bool) : create and modify SQL table and columns as needed if True
        '''

        self.__connection__ = connection
        self.__create__ = create.create(connection)
        self.__modify__ = modify.modify(connection)
        self.adjust_sql_objects = adjust_sql_objects
        
        self.insert()

