from typing import Literal

import pandas as pd

from mssql_dataframe.core import helpers


class create():

    def __init__(self, connection):
        '''Class for creating SQL tables.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        '''

        self.__connection__ = connection
        

    def table(self, table_name: str, columns: dict, not_null: list = [],
    primary_key_column: str = None, sql_primary_key: bool = False):
        """Create SQL table by explicitly specifying SQL create table parameters.

        Parameters
        ----------

        table_name (str) : name of table to create
        columns (dict) : keys = column names, values = data types and optionally size/precision
        not_null (list, default=[]) : list of columns to set as not null
        primary_key_column (str, default=None) : column to set as the primary key
        sql_primary_key (bool, default=False) : create an INT SQL identity column as the primary key named _pk

        Returns
        -------

        None

        Example
        -------
        
        create_table(table_name='##SingleColumnTable', columns={"A": "VARCHAR(100)"})

        """
        
        statement = """
        DECLARE @SQLStatement AS NVARCHAR(MAX);
        DECLARE @TableName SYSNAME = ?;
        {declare}
        SET @SQLStatement = N'CREATE TABLE '+QUOTENAME(@TableName)+' ('+
        {syntax}
        +');'
        EXEC sp_executesql
        @SQLStatement,
        N'@TableName SYSNAME, {parameters}',
        @TableName=@TableName, {values};
        """

        column_names = list(columns.keys())
        alias_names = [str(x) for x in list(range(0,len(column_names)))]
        size, dtypes = helpers.column_spec(columns.values())
        size_vars = [alias_names[idx] if x is not None else None for idx,x in enumerate(size)]

        # develop syntax for SQL variable declaration
        declare = list(zip(
            ["DECLARE @ColumnName_"+x+" SYSNAME = ?;" for x in alias_names],
            ["DECLARE @ColumnType_"+x+" SYSNAME = ?;" for x in alias_names],
            ["DECLARE @ColumnSize_"+x+" SYSNAME = ?;" if x is not None else "" for x in size_vars]
        ))
        declare = '\n'.join(['\n'.join(x) for x in declare])

        # develop syntax for SQL table creation
        if sql_primary_key and primary_key_column is not None:
            raise ValueError('if sql_primary_key==True then primary_key_column has to be None')
        syntax = list(zip(
            ["QUOTENAME(@ColumnName_"+x+")" for x in alias_names],
            ["QUOTENAME(@ColumnType_"+x+")" for x in alias_names],
            ["@ColumnSize_"+x+"" if x is not None else "" for x in size_vars],
            ["'NOT NULL'" if x in not_null else "" for x in column_names],
            ["'PRIMARY KEY'" if x==primary_key_column else "" for x in column_names]
        ))
        syntax = "+','+\n".join(
            ["+' '+".join([x for x in col if len(x)>0]) for col in syntax]
        )

        if sql_primary_key:
            syntax = "'_pk INT NOT NULL IDENTITY(1,1) PRIMARY KEY,'+\n"+syntax

        # develop syntax for sp_executesql parameters
        parameters = list(zip(
            ["@ColumnName_"+x+" SYSNAME" for x in alias_names],
            ["@ColumnType_"+x+" SYSNAME" for x in alias_names],
            ["@ColumnSize_"+x+" VARCHAR(MAX)" if x is not None else "" for x in size_vars]
        ))
        parameters = [", ".join([item for item in sublist if len(item)>0]) for sublist in parameters]
        parameters = ", ".join(parameters)

        # create input for sp_executesql SQL syntax
        values = list(zip(
            ["@ColumnName_"+x+""+"=@ColumnName_"+x+"" for x in alias_names],
            ["@ColumnType_"+x+""+"=@ColumnType_"+x+"" for x in alias_names],
            ["@ColumnSize_"+x+""+"=@ColumnSize_"+x+"" if x is not None else "" for x in size_vars]
        ))
        values = [", ".join([item for item in sublist if len(item)>0]) for sublist in values]
        values = ", ".join(values)

        # join components into final synax
        statement = statement.format(
            declare=declare,
            syntax=syntax,
            parameters=parameters,
            values=values
        )

        # create variables for execute method
        args = list(zip(
            [x for x in column_names],
            [x for x in dtypes],
            [x for x in size]
        ))
        args = [item for sublist in args for item in sublist if item is not None]
        args = [table_name] + args

        # execute statement
        helpers.execute(self.__connection__, statement, args)


    def table_from_dataframe(self, table_name: str, dataframe: pd.DataFrame, primary_key : Literal[None,'sql','index','infer'] = None, 
    row_count: int = 1000):
        """ Create SQL table by inferring SQL create table parameters from the contents of the DataFrame.

        Parameters
        ----------

        table_name (str) : name of table
        dataframe (DataFrame) : data used to create table
        primary_key (str, default = 'sql') : method of setting the table's primary key, see below for description of options
        row_count (int, default = 1000) : number of rows for determining data types

        primary_key = None : do not set a primary key
        primary_key = 'sql' : create an SQL managed auto-incrementing identity primary key column named '_pk'
        primary_key = 'index' : use the index of the dataframe and it's name, or '_index' if the index is not named
        primary_key = 'infer' : determine the column in the dataframe that best serves as a primary key and use it's name

        Returns
        -------

        None

        """

        options = [None,'sql','index','infer']
        if primary_key not in options:
            raise ValueError("primary_key must be one of: "+str(options))

        # determine primary key
        if primary_key is None:
            sql_primary_key = False
            primary_key_column = None
        elif primary_key == 'sql':
            sql_primary_key = True
            primary_key_column = None
        elif primary_key == 'index':
            sql_primary_key = False
            if dataframe.index.name is None:
                dataframe.index.name = '_index'
            primary_key_column = dataframe.index.name
            dataframe = dataframe.reset_index()
        elif primary_key == 'infer':
            sql_primary_key = False
            primary_key_column = None

        # not_null columns
        not_null = list(dataframe.columns[dataframe.notna().all()])

        # create temp table to determine data types
        name_temp = "##table_from_dataframe_"+table_name
 
        dtypes = helpers.infer_datatypes(self.__connection__, name_temp, dataframe, row_count)

        # infer primary key column after best fit data types have been determined
        if primary_key=='infer':
            # primary key must not be null
            subset = dataframe[not_null]
            # primary key must contain unique values
            unique = subset.nunique()==len(subset)
            unique = unique[unique].index
            subset = subset[unique]
            # attempt to use smallest integer
            primary_key_column = subset.select_dtypes(['int16', 'int32', 'int64']).columns
            if len(primary_key_column)>0:
                primary_key_column = subset[primary_key_column].max().idxmin()
            else:
                # attempt to use smallest float
                primary_key_column = subset.select_dtypes(['float16', 'float32', 'float64']).columns
                if len(primary_key_column)>0:
                    primary_key_column = subset[primary_key_column].max().idxmin()
                else:
                    # attempt to use shortest length string
                    primary_key_column = subset.select_dtypes(['object']).columns
                    if len(primary_key_column)>0:
                        primary_key_column = subset[primary_key_column].apply(lambda x: x.str.len()).max().idxmin()
                    else:
                        primary_key_column = None    

        # create final SQL table
        self.table(table_name, dtypes, not_null=not_null, primary_key_column=primary_key_column, sql_primary_key=sql_primary_key)

        # reset index after it was set as a column for table creation
        if primary_key=='index':
            dataframe = dataframe.set_index(keys=primary_key_column)
        
        return dataframe
    