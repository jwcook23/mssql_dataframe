"""A single class composed of insert, update, and merge classes."""
from mssql_dataframe.core.write.update import update
from mssql_dataframe.core.write.merge import merge


class write(update, merge):
    def __init__(
        self,
        connection,
        include_metadata_timestamps=False,
        autoadjust_sql_objects: bool = False,
    ):
        """Class for inserting, updating, and merging records between dataframes and SQL

        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statements
        include_metadata_timestamps (bool, default=False) : include metadata timestamps _time_insert & _time_update in server time for write operations
        autoadjust_sql_objects (bool, default=False) : create and modify SQL table and columns as needed if True
        """
        self._connection = connection.connection
        self.include_metadata_timestamps = include_metadata_timestamps
        self.autoadjust_sql_objects = autoadjust_sql_objects
