"""Methods for creating SQL tables both explicitly and implicitly."""
from typing import Literal, List, Dict
import warnings
import logging

import pandas as pd
import pyodbc

from mssql_dataframe.core import custom_warnings, dynamic, conversion, infer


class create:
    """Class for creating SQL tables both explicitly and implicitly."""

    def __init__(
        self, connection: pyodbc.connect, include_metadata_timestamps: bool = False
    ):
        """Class for creating SQL tables manually or automatically from a dataframe.

        Parameters
        ----------
        connection (pyodbc.Connection) : connection for executing statement
        include_metadata_timetstamps (bool, default=False) : if inserting data using table_from_dataframe, include _time_insert column
        """
        self._connection = connection
        self.include_metadata_timestamps = include_metadata_timestamps

    def table(
        self,
        table_name: str,
        columns: Dict[str, str],
        not_nullable: List[str] = [],
        primary_key_column: str = None,
        sql_primary_key: bool = False,
    ) -> None:
        """Create SQL table by explicitly specifying SQL create table parameters.

        Parameters
        ----------
        table_name (str) : name of table to create, may also contain schema name in the form schema_name.table_name
        columns (dict[str,str]) : keys = column names, values = data types and optionally size/precision if applicable
        not_nullable (list|str, default=[]) : columns to set as not null
        primary_key_column (str|list, default=None) : column(s) to set as the primary key, if a list a composite primary key is created
        sql_primary_key (bool, default=False) : create an SQL mananaged INT identity column as the primary key named _pk

        Returns
        -------
        None

        Examples
        --------
        #### simple table without primary key
        create.table(table_name='##CreateSimpleTable', columns={"A": "VARCHAR(100)"})

        #### table with a primary key and another non-nullable column
        create.table(table_name='##CreatePKTable', columns={"A": "VARCHAR(100)", "B": "INT"}, not_nullable="B", primary_key_column="A")

        #### table with an SQL identity primary key
        create.table(table_name='##CreateIdentityPKTable', columns={"A": "VARCHAR(100)", "B": "INT"}, not_nullable="B", sql_primary_key=True)
        """
        statement = """
        DECLARE @SQLStatement AS NVARCHAR(MAX);
        {declare}
        SET @SQLStatement = N'CREATE TABLE {table} ('+
        {syntax}
        {pk}
        +');'
        EXEC sp_executesql
        @SQLStatement,
        N'{parameters}',
        {values};
        """

        # check inputs
        if sql_primary_key and primary_key_column is not None:
            raise ValueError(
                "if sql_primary_key==True then primary_key_column has to be None"
            )
        if isinstance(not_nullable, str):
            not_nullable = [not_nullable]
        if isinstance(primary_key_column, str):
            primary_key_column = [primary_key_column]

        # parse inputs
        table_name = dynamic.escape(self._connection.cursor(), table_name)
        column_names = list(columns.keys())
        alias_names = [str(x) for x in list(range(0, len(column_names)))]
        size, dtypes_sql = dynamic.column_spec(columns.values())
        size_vars = [
            alias_names[idx] if x is not None else None for idx, x in enumerate(size)
        ]

        if primary_key_column is not None:
            missing = [x for x in primary_key_column if x not in columns]
            if missing:
                raise KeyError(
                    "primary_key_column is not in input varble columns", missing
                )
            alias_pk = [str(x) for x in list(range(0, len(primary_key_column)))]
        else:
            alias_pk = []

        # develop syntax for SQL variable declaration
        declare = list(
            zip(
                ["DECLARE @ColumnName_" + x + " SYSNAME = ?;" for x in alias_names],
                ["DECLARE @ColumnType_" + x + " SYSNAME = ?;" for x in alias_names],
                [
                    "DECLARE @ColumnSize_" + x + " SYSNAME = ?;"
                    if x is not None
                    else ""
                    for x in size_vars
                ],
            )
        )
        declare = "\n".join(["\n".join(x) for x in declare])
        if primary_key_column is not None:
            declare += "\n" + "\n".join(
                ["DECLARE @PK_" + x + " SYSNAME = ?;" for x in alias_pk]
            )

        # develop syntax for SQL table creation
        syntax = list(
            zip(
                ["QUOTENAME(@ColumnName_" + x + ")" for x in alias_names],
                ["QUOTENAME(@ColumnType_" + x + ")" for x in alias_names],
                ["@ColumnSize_" + x + "" if x is not None else "" for x in size_vars],
                ["'NOT NULL'" if x in not_nullable else "" for x in column_names],
            )
        )
        syntax = "+','+\n".join(
            ["+' '+".join([x for x in col if len(x) > 0]) for col in syntax]
        )

        # primary key syntax
        pk = ""
        if sql_primary_key:
            syntax = "'_pk INT NOT NULL IDENTITY(1,1) PRIMARY KEY,'+\n" + syntax
        elif primary_key_column is not None:
            pk = "+','+".join(["QUOTENAME(@PK_" + x + ")" for x in alias_pk])
            pk = "+\n',PRIMARY KEY ('+" + pk + "+')'"

        # develop syntax for sp_executesql parameters
        parameters = list(
            zip(
                ["@ColumnName_" + x + " SYSNAME" for x in alias_names],
                ["@ColumnType_" + x + " SYSNAME" for x in alias_names],
                [
                    "@ColumnSize_" + x + " VARCHAR(MAX)" if x is not None else ""
                    for x in size_vars
                ],
            )
        )
        parameters = [
            ", ".join([item for item in sublist if len(item) > 0])
            for sublist in parameters
        ]
        parameters = ", ".join(parameters)
        if primary_key_column is not None:
            parameters += ", " + ", ".join(["@PK_" + x + " SYSNAME" for x in alias_pk])

        # create input for sp_executesql SQL syntax
        values = list(
            zip(
                [
                    "@ColumnName_" + x + "" + "=@ColumnName_" + x + ""
                    for x in alias_names
                ],
                [
                    "@ColumnType_" + x + "" + "=@ColumnType_" + x + ""
                    for x in alias_names
                ],
                [
                    "@ColumnSize_" + x + "" + "=@ColumnSize_" + x + ""
                    if x is not None
                    else ""
                    for x in size_vars
                ],
            )
        )
        values = [
            ", ".join([item for item in sublist if len(item) > 0]) for sublist in values
        ]
        values = ", ".join(values)
        if primary_key_column is not None:
            values += ", " + ", ".join(
                ["@PK_" + x + "" + "=@PK_" + x + "" for x in alias_pk]
            )

        # join components into final synax
        statement = statement.format(
            table=table_name,
            declare=declare,
            syntax=syntax,
            pk=pk,
            parameters=parameters,
            values=values,
        )

        # create variables for execute method
        args = list(
            zip([x for x in column_names], [x for x in dtypes_sql], [x for x in size])
        )
        args = [item for sublist in args for item in sublist if item is not None]
        if primary_key_column is not None:
            args += primary_key_column

        # execute statement
        cursor = self._connection.cursor()
        cursor.execute(statement, args)
        cursor.commit()

    def table_from_dataframe(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        primary_key: Literal[None, "sql", "index", "infer"] = None,
        insert_dataframe: bool = True,
    ) -> pd.DataFrame:
        """Create SQL table by inferring SQL create table parameters from the contents of a dataframe.

        The contents can be composed of strings/objects only and converted better data types if conversion is possible within pandas.

        Parameters
        ----------
        table_name (str) : name of table to create, may also contain schema name in the form schema_name.table_name
        dataframe (pandas.DataFrame) : data used to create table
        primary_key (str, default = None) : method of setting the table's primary key, see below for description of options
        insert_dataframe (bool, default=True) : insert the dataframe after creating the table

        primary_key = None : do not set a primary key
        primary_key = 'sql' : create an SQL managed auto-incrementing INT identity primary key column named '_pk'
        primary_key = 'index' : use the index of the dataframe and it's name, or '_index' if the index is not named
        primary_key = 'infer' : determine the column in the dataframe that best serves as a primary key and use it's name

        Returns
        -------
        dataframe (pandas.DataFrame) : data potentially converted from obejcts/strings to better pandas types

        Examples
        --------
        #### create table without a primary key
        df = create.table_from_dataframe('##DFNoPK', pd.DataFrame({"ColumnA": [1]}))

        #### create table with the dataframe's index as the primary key
        df = create.table_from_dataframe('##DFIndexPK', pd.DataFrame({"ColumnA": [1,2]}, index=['a','z']), primary_key='index')

        #### create an SQL identity primary key
        df = create.table_from_dataframe('##DFIdentityPK', pd.DataFrame({"ColumnA": [1,2]}), primary_key='sql')

        #### create table using ColumnA as the primary key, after it was inferred to be the primary key
        df = create.table_from_dataframe('##DFInferPK', pd.DataFrame({"ColumnA": [1,2], "ColumnB": ["a","b"]}), primary_key='infer')
        """
        # determine primary key
        if primary_key is None:
            sql_primary_key = False
            primary_key_column = None
        elif primary_key == "sql":
            sql_primary_key = True
            primary_key_column = None
        elif primary_key == "index":
            sql_primary_key = False
            if not any(dataframe.index.names):
                dataframe.index.name = "_index"
            primary_key_column = list(dataframe.index.names)
            dataframe = dataframe.reset_index()
        elif primary_key == "infer":
            sql_primary_key = False
            primary_key_column = None
        else:
            options = [None, "sql", "index", "infer"]
            raise ValueError("primary_key must be one of: " + str(options))

        # infer SQL specifications from contents of dataframe
        dataframe, schema, not_nullable, pk = infer.sql(dataframe)
        _, dtypes = conversion.sql_spec(schema, dataframe)

        # infer primary key column after best fit data types have been determined
        if primary_key == "infer":
            primary_key_column = pk

        # add _time_insert column
        if self.include_metadata_timestamps:
            dtypes["_time_insert"] = "DATETIME2"

        # create final SQL table
        self.table(
            table_name,
            dtypes,
            not_nullable=not_nullable,
            primary_key_column=primary_key_column,
            sql_primary_key=sql_primary_key,
        )

        # issue message for derived table
        pk_name = primary_key_column
        if sql_primary_key:
            pk_name = "_pk (SQL managed int identity column)"
        elif primary_key == "index":
            pk_name = str(primary_key_column) + " (dataframe index)"
        elif primary_key_column is not None:
            pk_name = primary_key_column + " (dataframe column)"
        else:
            pk_name = "None"
        msg = f"""
        Created table: {table_name}
        Primary key: {pk_name}
        Non-null columns: {not_nullable}
        Data types: {dtypes}
        """
        warnings.warn(msg, custom_warnings.SQLObjectAdjustment)
        logging.warning(msg)

        # set primary key column as dataframe index
        if primary_key_column is not None:
            dataframe = dataframe.set_index(keys=primary_key_column)

        # insert dataframe
        if insert_dataframe:
            cursor = self._connection.cursor()
            cursor.fast_executemany = True
            dataframe = conversion.insert_values(
                table_name, dataframe, self.include_metadata_timestamps, schema, cursor
            )

        return dataframe
