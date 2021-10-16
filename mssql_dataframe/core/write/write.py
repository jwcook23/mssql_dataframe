"""A single class composed of insert, update, and merge classes."""
import pyodbc

from mssql_dataframe.core import modify, create
from mssql_dataframe.core.write.update import update
from mssql_dataframe.core.write.merge import merge


class write(update, merge):
    def __init__(
        self,
        connection: pyodbc.connect,
        include_metadata_timestamps: bool = False,
        autoadjust_sql_objects: bool = False,
    ):
        """Class for inserting, updating, and merging records between dataframes and SQL

        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statements
        include_metadata_timestamps (bool, default=False) : include metadata timestamps _time_insert & _time_update in server time for write operations
        autoadjust_sql_objects (bool, default=False) : create and modify SQL table and columns as needed if True
        """
        self._connection = connection
        self.include_metadata_timestamps = include_metadata_timestamps
        self.autoadjust_sql_objects = autoadjust_sql_objects

        # max attempts for creating/modifing SQL tables
        # value of 3 will: add include_metadata_timestamps columns and/or add other columns and/or increase column size
        self._adjust_sql_attempts = 3

        # handle failures if autoadjust_sql_objects==True
        self._modify = modify.modify(connection)
        self._create = create.create(connection)
