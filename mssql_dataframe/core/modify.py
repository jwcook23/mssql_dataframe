"""Methods for modifying SQL columns or primary keys."""

from typing import Literal, List
import pyodbc

from mssql_dataframe.core import dynamic, conversion


class modify:
    """Class for modifying SQL columns or primary keys."""

    def __init__(self, connection: pyodbc.connect):
        """Class for modifying SQL table columns.

        Parameters
        ----------
        connection (pyodbc.Connection) : connection for executing statement
        """
        self._connection = connection

    def column(
        self,
        table_name: str,
        modify: Literal["add", "alter", "drop"],
        column_name: str,
        data_type: str = None,
        is_nullable: bool = True,
    ) -> None:
        """Add, alter, or drop a column in an existing SQL table.

        Parameters
        ----------
        table_name (str) : name of table
        modify (str) : method of modification, see below for description of options
        column_name (str) : name of column
        data_type (str) : if modify='add' or modify='alter', data type and optionally size/precision
        is_nullable (bool, default=True) : if modify='alter', specification for if the column is nullable

        modify = 'add' : adds the column to the table
        modify = 'alter' : change the data type or nullability of the column
        modify = 'drop' : removes the column from the table

        Returns
        -------
        None

        Example
        -------
        A sample table to modify.
        >>> create.table('##ExampleModifyTableColumn', columns={'ColumnA': 'varchar(1)'})

        Add a column to a table.
        >>> modify.column('##ExampleModifyTableColumn', modify='add', column_name='B', data_type='bigint')

        Alter an existing column.
        >>> modify.column('##ExampleModifyTableColumn', modify='alter', column_name='B', data_type='tinyint', is_nullable=False)

        Drop a column.
        >>> modify.column('##ExampleModifyTableColumn', modify='drop', column_name='B')
        """
        statement = """
            DECLARE @SQLStatement AS NVARCHAR(MAX);
            DECLARE @SchemaName SYSNAME = ?;
            DECLARE @TableName SYSNAME = ?;
            DECLARE @ColumnName SYSNAME = ?;
            {declare_type}
            {declare_size}

            SET @SQLStatement =
                N'ALTER TABLE '+QUOTENAME(@SchemaName)+'.'+QUOTENAME(@TableName)+
                {syntax} +QUOTENAME(@ColumnName) {type_column} {size_column} {null_column}+';'

            EXEC sp_executesql
                @SQLStatement,
                N'@SchemaName SYSNAME, @TableName SYSNAME, @ColumnName SYSNAME {parameter_type} {parameter_size}',
                @SchemaName=@SchemaName, @TableName=@TableName, @ColumnName=@ColumnName {value_type} {value_size};
        """

        schema_name, table_name = conversion._get_schema_name(table_name)

        args = [schema_name, table_name, column_name]
        if modify == "drop":
            syntax = "'DROP COLUMN'"
            declare_type = ""
            declare_size = ""
            type_column = ""
            size_column = ""
            null_column = ""
            parameter_type = ""
            parameter_size = ""
            value_type = ""
            value_size = ""
        elif modify == "add" or modify == "alter":
            if modify == "add":
                syntax = "'ADD'"
            else:
                # modify == "alter"
                syntax = "'ALTER COLUMN'"
            declare_type = "DECLARE @ColumnType SYSNAME = ?;"
            type_column = "+' '+QUOTENAME(@ColumnType)"
            parameter_type = ", @ColumnType SYSNAME"
            value_type = ", @ColumnType=@ColumnType"
            size, dtypes_sql = dynamic.column_spec(data_type)
            if size is None:
                declare_size = ""
                size_column = ""
                parameter_size = ""
                value_size = ""
            else:
                declare_size = "DECLARE @ColumnSize SYSNAME = ?;"
                size_column = "+' '+@ColumnSize"
                parameter_size = ", @ColumnSize VARCHAR(MAX)"
                value_size = ", @ColumnSize=@ColumnSize"
            if is_nullable:
                null_column = ""
            else:
                null_column = "+' NOT NULL'"

            args += [dtypes_sql, size]
        else:
            options = ["add", "alter", "drop"]
            raise ValueError("modify must be one of: " + str(options))

        statement = statement.format(
            declare_type=declare_type,
            declare_size=declare_size,
            syntax=syntax,
            type_column=type_column,
            size_column=size_column,
            null_column=null_column,
            parameter_type=parameter_type,
            parameter_size=parameter_size,
            value_type=value_type,
            value_size=value_size,
        )

        args = [x for x in args if x is not None]
        cursor = self._connection.cursor()
        cursor.execute(statement, *args)

    def primary_key(
        self,
        table_name: str,
        modify: Literal["add", "drop"],
        columns: List[str],
        primary_key_name: str,
    ) -> None:
        """Add or drop the primary key from a table.

        Parameters
        ----------
        table_name (str) : name of the table to add/drop the primary key
        key_name (str) : name of the primary key to add/drop
        columns (list|str) : name of the column(s) to add/drop as the primary key
        modify (str) : specification to either add or drop the primary key
        primary_key_name (str) : name of the primary key

        Returns
        -------
        None

        Examples
        --------
        A sample table to modify.
        >>> create.table('##ExampleModifyTablePK', columns={'ColumnA': 'varchar(1)'}, not_nullable=['ColumnA'])

        Add a primary key.
        >>> modify.primary_key('##ExampleModifyTablePK', modify='add', columns='ColumnA', primary_key_name = '_pk_1')

        Drop a primary key.
        >>> modify.primary_key('##ExampleModifyTablePK', modify='drop', columns='ColumnA',  primary_key_name = '_pk_1')
        """
        if isinstance(columns, str):
            columns = [columns]

        statement = """
            DECLARE @SQLStatement AS NVARCHAR(MAX);
            DECLARE @TableName SYSNAME = ?;
            DECLARE @PrimaryKeyName SYSNAME = ?;
            {declare}

            SET @SQLStatement =
                N'ALTER TABLE '+QUOTENAME(@TableName)+
                {syntax} + QUOTENAME(@PrimaryKeyName) {keys} +';'
            EXEC sp_executesql
                @SQLStatement,
                N'@TableName SYSNAME, @PrimaryKeyName SYSNAME {parameter}',
                @TableName=@TableName, @PrimaryKeyName=@PrimaryKeyName {value};
        """

        args = [table_name, primary_key_name]
        if modify == "add":
            args += columns
            declare = "\n".join(
                [
                    "DECLARE @PK" + str(idx) + " SYSNAME = ?;"
                    for idx, _ in enumerate(columns)
                ]
            )
            syntax = "'ADD CONSTRAINT '"
            keys = "+','+".join(
                ["QUOTENAME(@PK" + str(idx) + ")" for idx, _ in enumerate(columns)]
            )
            keys = "+'PRIMARY KEY ('+" + keys + "+')'"
            parameter = " ".join(
                [", @PK" + str(idx) + " SYSNAME" for idx, _ in enumerate(columns)]
            )
            value = " ".join(
                [
                    ", @PK" + str(idx) + "=@PK" + str(idx)
                    for idx, _ in enumerate(columns)
                ]
            )
        elif modify == "drop":
            declare = ""
            syntax = "'DROP CONSTRAINT '"
            keys = ""
            parameter = ""
            value = ""
        else:
            options = ["add", "drop"]
            raise ValueError("modify must be one of: " + str(options))
        statement = statement.format(
            declare=declare, syntax=syntax, keys=keys, parameter=parameter, value=value
        )

        cursor = self._connection.cursor()
        cursor.execute(statement, *args)
