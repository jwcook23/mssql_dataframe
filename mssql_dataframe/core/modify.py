from typing import Literal

from mssql_dataframe.core import helpers


class modify():

    def __init__(self, connection):
        '''Class for modifying SQL table properties.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        '''

        self.__connection__ = connection


    def column(self, table_name: str, modify: Literal['add','alter','drop'], column_name: str, data_type: str = None, not_null=False):
        """Add, alter, or drop a column in an existing SQL table.

        Parameters
        ----------

        table_name (str) : name of table
        modify (str) : method of modification, see below for description of options
        column_name (str) : name of column
        data_type (str) : if modify='add' or modify='alter', data type and optionally size/precision
        not_null (bool, default=False) : if modify='alter', specification for if the column is nullable

        modify = 'add' : adds the column to the table
        modify = 'alter' : change the data type or nullability of the column
        modify = 'drop' : removes the column from the table

        Returns
        -------

        None

        Example
        -------
        
        TODO: add example

        """

        options = ['add','alter','drop']
        if modify not in options:
            raise ValueError("modify must be one of: "+str(options))
        
        statement = """
            DECLARE @SQLStatement AS NVARCHAR(MAX);
            DECLARE @TableName SYSNAME = ?;
            DECLARE @ColumnName SYSNAME = ?;
            {declare_type}
            {declare_size}

            SET @SQLStatement = 
                N'ALTER TABLE '+QUOTENAME(@TableName)+
                {syntax} +QUOTENAME(@ColumnName) {type_column} {size_column} {null_column}+';'

            EXEC sp_executesql 
                @SQLStatement,
                N'@TableName SYSNAME, @ColumnName SYSNAME {parameter_type} {parameter_size}',
                @TableName=@TableName, @ColumnName=@ColumnName {value_type} {value_size};
        """

        args = [table_name, column_name]
        if modify=='drop':
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
        elif modify=='add' or modify=='alter':
            if modify=='add':
                syntax = "'ADD'"
            elif modify=='alter':
                syntax = "'ALTER COLUMN'"
            declare_type = "DECLARE @ColumnType SYSNAME = ?;"
            type_column = "+' '+QUOTENAME(@ColumnType)"
            parameter_type = ", @ColumnType SYSNAME"
            value_type = ", @ColumnType=@ColumnType"
            size, dtypes = helpers.column_spec(data_type)
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
            if not_null:
                null_column = "+' NOT NULL'"
            else:
                null_column = ""
            
            args += [dtypes, size]

        statement = statement.format(
            declare_type=declare_type, declare_size=declare_size,
            syntax=syntax, 
            type_column=type_column, size_column=size_column, null_column=null_column,
            parameter_type=parameter_type, parameter_size=parameter_size,
            value_type=value_type, value_size=value_size
        )

        args = [x for x in args if x is not None]
        _ = helpers.execute(self.__connection__, statement, args)


    def primary_key(self, table_name: str, modify: Literal['add','drop'], columns: list, primary_key_name: str):
        '''Add or drop the primary key from a table.

        Parameters
        ----------

        table_name (str) : name of the table to add/drop the primary key
        key_name (str) : name of the primary key to add/drop
        colums (list|str) : name of the column(s) to add/drop as the primary key
        modify (str) : specification to either add or drop the primary key
        primary_key_name (str) : name of the primary key

        Returns
        -------

        None
        '''

        options = ['add','drop']
        if modify not in options:
            raise ValueError("modify must be one of: "+str(options))

        if isinstance(columns,str):
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
        if modify=='add':
            args += columns
            declare = "\n".join(["DECLARE @PK"+str(idx)+" SYSNAME = ?;" for idx,_ in enumerate(columns)])
            syntax = "'ADD CONSTRAINT '"
            keys = "+','+".join(["QUOTENAME(@PK"+str(idx)+")" for idx,_ in enumerate(columns)])
            keys = "+'PRIMARY KEY ('+"+keys+"+')'"
            parameter = " ".join([", @PK"+str(idx)+" SYSNAME" for idx,_ in enumerate(columns)])
            value = " ".join([", @PK"+str(idx)+"=@PK"+str(idx) for idx,_ in enumerate(columns)])
        elif modify=='drop':
            declare = ""
            syntax = "'DROP CONSTRAINT '"
            keys = ""
            parameter = ""
            value = ""
        statement = statement.format(declare=declare, 
            syntax=syntax, keys=keys, 
            parameter=parameter, value=value
        )

        _ = helpers.execute(self.__connection__, statement, args)
