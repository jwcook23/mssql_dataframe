import warnings

from mssql_dataframe.core import create, modify, errors
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

        if self.adjust_sql_objects:
            warnings.warn('write class initialized with adjust_sql_objects=True', errors.SQLObjectAdjustment)

        
        self.insert()

