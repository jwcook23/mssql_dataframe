"""Methods for reading from SQL into a dataframe."""

from typing import Literal

import pandas as pd
import pyodbc

from mssql_dataframe.core import custom_errors, dynamic, conversion


class read:
    """Class for reading from SQL into a dataframe."""

    def __init__(self, connection: pyodbc.connect):
        """Class for reading from SQL tables.

        Parameters
        ----------
        connection (pyodbc.Connection) : connection for executing statement
        """
        self._connection = connection

    def table(
        self,
        table_name: str,
        column_names: list = None,
        where: str = None,
        limit: int = None,
        order_column: str = None,
        order_direction: Literal[None, "ASC", "DESC"] = None,
    ) -> pd.DataFrame:
        """Select data from SQL into a dataframe.

        Parameters
        ----------
        table_name (str) : name of table to select data frame
        column_names (list|str, default=None) : list of columns to select, or None to select all
        where (str, default=None) : where clause filter to apply
        limit (int, default=None) : select limited number of records only
        order_column (str, default=None) : order results by column
        order_direction (str, default=None) : order direction

        Returns
        -------
        dataframe (pandas.DataFrame): tabular data from select statement

        Examples
        --------
        A sample table to read, created from a dataframe.
        >>> df = pd.DataFrame(
        ...    {
        ...        "ColumnA": [5, 6, None],
        ...        "ColumnB": ["06-22-2021", "06-22-2021", pd.NaT],
        ...        "ColumnC": ["aa", "b", None],
        ...    }, index = pd.Index(["xxx", "yyy", "zzz"], name='PK')
        ... )
        >>> create.table('##ExampleRead',
        ...     {
        ...         'ColumnA': 'TINYINT', 'ColumnB': 'DATETIME2', 'ColumnC': 'VARCHAR(2)', 'PK': 'CHAR(3)'
        ...     },
        ...     primary_key_column = 'PK'
        ... )
        >>> df = insert('##ExampleRead', df)

        Select the entire table. The primary key is set as the dataframe's index.
        >>> query = read.table('##ExampleRead')

        Select specific columns.
        >>> query = read.table('##ExampleRead', column_names=['ColumnA','ColumnB'])

        Select using conditions grouped by parentheses while applying a limit and order.
        >>> query = read.table('##ExampleRead', where="(ColumnA>5 AND ColumnB IS NOT NULL) OR ColumnC IS NULL", limit=5, order_column='ColumnB', order_direction='DESC')
        """
        # get table schema for conversion to pandas
        schema, _ = conversion.get_schema(self._connection, table_name)

        # always read in primary key columns for dataframe index
        primary_key_columns = list(
            schema.loc[schema["pk_seq"].notna(), "pk_seq"]
            .sort_values(ascending=True)
            .index
        )

        # dynamic table and column names, and column_name development
        table_name = dynamic.escape(self._connection.cursor(), table_name)
        if column_names is None:
            column_names = "*"
        else:
            if isinstance(column_names, str):
                column_names = [column_names]
            elif isinstance(column_names, pd.Index):
                column_names = list(column_names)
            column_names = primary_key_columns + column_names
            column_names = list(set(column_names))
            missing = [x for x in column_names if x not in schema.index]
            if len(missing) > 0:
                raise custom_errors.SQLColumnDoesNotExist(
                    f"Column does not exist in table {table_name}:", missing
                )
            column_names = dynamic.escape(self._connection.cursor(), column_names)
            column_names = "\n,".join(column_names)

        # format optional where_statement
        if where is None:
            where_statement, where_args = ("", None)
        else:
            where_statement, where_args = dynamic.where(
                self._connection.cursor(), where
            )

        # format optional limit
        if limit is None:
            limit = ""
        elif not isinstance(limit, int):
            raise ValueError("limit must be an integer")
        else:
            limit = "TOP(" + str(limit) + ")"

        # format optional order
        options = [None, "ASC", "DESC"]
        if (order_column is None and order_direction is not None) or (
            order_column is not None and order_direction is None
        ):
            raise ValueError("order_column and order_direction must both be specified")
        elif order_direction not in options:
            raise ValueError("order direction must be one of: " + str(options))
        elif order_column is not None:
            order = (
                "ORDER BY "
                + dynamic.escape(self._connection.cursor(), order_column)
                + " "
                + order_direction
            )
        else:
            order = ""

        # skip security check since table_name, column_names, where_statement, order have been escaped
        # limit has been enforced to be an integer
        statement = f"""
        SELECT {limit}
            {column_names}
        FROM
            {table_name}
            {where_statement}
            {order}
        """  # nosec hardcoded_sql_expressions

        # read sql query
        dataframe = conversion.read_values(
            statement, schema, self._connection, where_args
        )

        return dataframe
