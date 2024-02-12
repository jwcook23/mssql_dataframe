"""Methods to insert, update, and merge dataframes."""

import pyodbc

from mssql_dataframe.core import modify, create
from mssql_dataframe.core.write.update import update
from mssql_dataframe.core.write.merge import merge


class write(update, merge):
    """Class for inserting, updating, and merging records between dataframes and SQL.

    Parameters
    ----------
    connection (mssql_dataframe.connect) : connection for executing statements
    include_metadata_timestamps (bool, default=False) : include metadata timestamps _time_insert & _time_update in server time for write operations
    """

    def __init__(
        self,
        connection: pyodbc.connect,
        include_metadata_timestamps: bool = False,
    ):
        self._connection = connection
        self.include_metadata_timestamps = include_metadata_timestamps

        # create temporary table for update/upsert/merge
        self._create = create.create(connection)

        # create include_metadata_timestamps
        self._modify = modify.modify(connection)
