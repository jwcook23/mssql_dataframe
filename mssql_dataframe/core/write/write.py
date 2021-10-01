from mssql_dataframe.core.write.update import update
from mssql_dataframe.core.write.merge import merge


class write(update, merge):
    def __init__(self, connection, adjust_sql_objects: bool = False):

        self._connection = connection
        self.adjust_sql_objects = adjust_sql_objects
