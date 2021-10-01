"""A single class composed of insert, update, and merge classes."""
from mssql_dataframe.core.write.update import update
from mssql_dataframe.core.write.merge import merge


class write(update, merge):
    def __init__(self, connection, auto_adjust_sql_objects: bool = False):

        self._connection = connection
        self.auto_adjust_sql_objects = auto_adjust_sql_objects
