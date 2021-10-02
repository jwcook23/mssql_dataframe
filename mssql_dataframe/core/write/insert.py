"""Class for inserting data into SQL."""
from mssql_dataframe.core import errors, conversion, dynamic, modify, create
from mssql_dataframe.core.write import _exceptions

from typing import Tuple, List

import pandas as pd
import pyodbc

pd.options.mode.chained_assignment = "raise"


class insert:
    def __init__(
        self,
        connection,
        include_metadata_timestamps: bool = False,
        autoadjust_sql_objects: bool = False,
    ):
        """Class for inserting data into SQL.

        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        include_metadata_timestamps (bool, default=False) : include metadata timestamps _time_insert & _time_update for write operations
        autoadjust_sql_objects (bool, default=False) : if True, create SQL tables or alter SQL columns if needed

        """

        self._connection = connection.connection
        self.include_metadata_timestamps = include_metadata_timestamps
        self.autoadjust_sql_objects = autoadjust_sql_objects

        # max attempts for creating/modifing SQL tables
        # value of 3 will: add include_metadata_timestamps columns and/or add other columns and/or increase column size
        self._adjust_sql_attempts = 3

        # handle failures if autoadjust_sql_objects==True
        self._modify = modify.modify(connection)
        self._create = create.create(connection)

    def insert(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        include_metadata_timestamps: bool = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Insert data into SQL table from a dataframe.

        Parameters
        ----------
        table_name (str) : name of table to insert data into
        dataframe (pandas.DataFrame): tabular data to insert

        Returns
        -------
        dataframe (pandas.DataFrame) : input dataframe that may have been altered to conform to SQL
        schema (pandas.DataFrame) : properties of SQL table columns where data was inserted
        include_metadata_timestamps (bool, default=None) : override for the class initialized parameter autoadjust_sql_objects

        Examples
        --------
        #### insert a dataframe into a table
        write.insert('SomeTable', pd.DataFrame({'ColumnA': [1, 2, 3]}))

        """

        # create cursor to perform operations
        cursor = self._connection.cursor()
        cursor.fast_executemany = True

        # override self.include_metadata_timestamps, for the update class to insert into a source temp table
        if include_metadata_timestamps is None:
            include_metadata_timestamps = self.include_metadata_timestamps

        # get target table schema, while checking for errors and adjusting data for inserting
        if include_metadata_timestamps:
            additional_columns = ["_time_insert"]
        else:
            additional_columns = None
        schema, dataframe = self._target_table(
            table_name, dataframe, cursor, additional_columns
        )

        # column names from dataframe contents
        if any(dataframe.index.names):
            # named index columns will also have values returned from conversion.prepare_values
            columns = list(dataframe.index.names) + list(dataframe.columns)
        else:
            columns = dataframe.columns

        # dynamic SQL object names
        table = dynamic.escape(cursor, table_name)
        columns = dynamic.escape(cursor, columns)

        # prepare values of dataframe for insert
        dataframe, values = conversion.prepare_values(schema, dataframe)

        # prepare cursor for input data types and sizes
        cursor = conversion.prepare_cursor(schema, dataframe, cursor)

        # issue insert statement
        if include_metadata_timestamps:
            insert = "_time_insert, " + ", ".join(columns)
            params = "GETDATE(), " + ", ".join(["?"] * len(columns))
        else:
            insert = ", ".join(columns)
            params = ", ".join(["?"] * len(columns))
        statement = f"""
        INSERT INTO
        {table} (
            {insert}
        ) VALUES (
            {params}
        )
        """
        cursor.executemany(statement, values)
        cursor.commit()

        return dataframe, schema

    def _target_table(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        cursor: pyodbc.connect,
        additional_columns: List[str] = None,
        updating_table: bool = False,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get target schema, potentially handle errors, and adjust dataframe contents before inserting into target table.

        Parameters
        ----------
        table_name (str) : name of target table
        dataframe (pandas.DataFrame): tabular data to insert
        cursor (pyodbc.connection.cursor) : cursor to execute statement
        additional_columns (list, default=None) : columns that will be generated by an SQL statement but not in the dataframe
        updating_table (bool, default=False) : flag that indicates if target table is being updated

        Returns
        -------
        schema (pandas.DataFrame) : table column specifications and conversion rules
        dataframe (pandas.DataFrame) : input dataframe with optimal values and types for inserting into SQL
        """

        for attempt in range(0, self._adjust_sql_attempts + 1):
            try:
                schema, dataframe = conversion.get_schema(
                    self._connection,
                    table_name,
                    dataframe,
                    additional_columns,
                )
                break
            except (
                errors.SQLTableDoesNotExist,
                errors.SQLColumnDoesNotExist,
                errors.SQLInsufficientColumnSize,
            ) as failure:
                cursor.rollback()
                if attempt == self._adjust_sql_attempts:
                    raise RecursionError(
                        f"adjust_sql_attempts={self._adjust_sql_attempts} reached"
                    )
                dataframe = _exceptions.handle(
                    failure,
                    table_name,
                    dataframe,
                    updating_table,
                    self.autoadjust_sql_objects,
                    self._modify,
                    self._create,
                )
                cursor.commit()
            except Exception as err:
                cursor.rollback()
                raise err

        return schema, dataframe

    def _source_table(
        self,
        table_name,
        dataframe,
        cursor,
        match_columns: list = None,
        additional_columns: list = None,
        updating_table: bool = False,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, list[str], str]:
        """Create a source table with data in SQL for update and merge operations.

        Parameters
        ----------
        table_name (str) : name of target table
        dataframe (pandas.DataFrame): tabular data to insert
        cursor (pyodbc.connection.cursor) : cursor to execute statement
        match_columns (list|str) : columns to match records to updating/merging, if None the primary key is used
        additional_columns (list, default=None) : columns that will be generated by an SQL statement but not in the dataframe
        updating_table (bool, default=False) : flag that indicates if target table is being updated

        Returns
        -------
        schema (pandas.DataFrame) : table column specifications and conversion rules
        dataframe (pandas.DataFrame) : input dataframe with optimal values and types for inserting into SQL
        match_columns (list) : columns used to perform matching between source and target tables
        temp_name (str) : name of the source temporary table that was created

        """
        if isinstance(match_columns, str):
            match_columns = [match_columns]

        # get target table schema, while checking for errors and adjusting data for inserting
        schema, dataframe = self._target_table(
            table_name, dataframe, cursor, additional_columns, updating_table
        )

        # use primary key if match_columns is not given
        if match_columns is None:
            match_columns = list(schema[schema["pk_seq"].notna()].index)
            if not match_columns:
                raise errors.SQLUndefinedPrimaryKey(
                    "SQL table {} has no primary key. Either set the primary key or specify the match_columns".format(
                        table_name
                    )
                )
        # match_column presence in dataframe
        missing = [
            x
            for x in match_columns
            if x not in list(dataframe.index.names) + list(dataframe.columns)
        ]
        if missing:
            raise errors.DataframeColumnDoesNotExist(
                "match_columns not found in dataframe", missing
            )

        # insert data into source temporary table
        temp_name = "##__source_" + table_name
        columns = list(dataframe.columns)
        if any(dataframe.index.names):
            columns = list(dataframe.index.names) + columns
        _, dtypes = conversion.sql_spec(schema.loc[columns], dataframe)
        dtypes = {k: v.replace("int identity", "int") for k, v in dtypes.items()}
        not_nullable = list(schema[~schema["is_nullable"]].index)
        self._create.table(
            temp_name, dtypes, not_nullable, primary_key_column=match_columns
        )
        _, _ = self.insert(temp_name, dataframe, include_metadata_timestamps=False)

        return schema, dataframe, match_columns, temp_name
