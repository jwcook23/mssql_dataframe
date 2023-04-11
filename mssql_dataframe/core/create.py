"""Methods for creating SQL tables both explicitly and implicitly."""
from typing import Literal, List, Dict
import logging

import pandas as pd
import pyodbc

from mssql_dataframe.core import dynamic, conversion

logger = logging.getLogger(__name__)


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
        Simple table without primary key.
        >>> create.table(table_name='##ExampleCreateTable', columns={"A": "VARCHAR(100)"})

        Table with a primary key and another non-nullable column.

        >>> create.table(table_name='##ExampleCreatePKTable', columns={"A": "VARCHAR(100)", "B": "INT"}, not_nullable="B", primary_key_column="A")

        Table with an SQL identity primary key.

        >>> create.table(table_name='##ExampleCreateIdentityPKTable', columns={"A": "VARCHAR(100)", "B": "INT"}, not_nullable="B", sql_primary_key=True)
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
