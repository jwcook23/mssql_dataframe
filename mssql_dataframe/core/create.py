from typing import Literal
import warnings

import pandas as pd

from mssql_dataframe.core import dynamic, errors, conversion, infer


class create():

    def __init__(self, connection):
        '''Class for creating SQL tables manually or automatically from a dataframe.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        '''

        self.connection = connection
        

    def table(self, table_name: str, columns: dict, not_nullable: list = [],
    primary_key_column: str = None, sql_primary_key: bool = False):
        """Create SQL table by explicitly specifying SQL create table parameters.

        Parameters
        ----------

        table_name (str) : name of table to create
        columns (dict) : keys = column names, values = data types and optionally size/precision
        not_nullable (list|str, default=[]) : list of columns to set as not null or a single column
        primary_key_column (str|list, default=None) : column(s) to set as the primary key
        sql_primary_key (bool, default=False) : create an INT SQL identity column as the primary key named _pk

        Returns
        -------

        None

        Examples
        -------
        
        #### simple table without primary key
        create.table(table_name='##CreateSimpleTable', columns={"A": "VARCHAR(100)"})

        #### table with a primary key and another not null column
        create.table(table_name='##CreatePKTable', columns={"A": "VARCHAR(100)", "B": "INT"}, not_nullable="B", primary_key_column="A")

        #### table with an SQL identity primary key
        create.table(table_name='##CreateIdentityPKTable', columns={"A": "VARCHAR(100)", "B": "INT"}, not_nullable="B", sql_primary_key=True)

        """
        
        statement = """
        DECLARE @SQLStatement AS NVARCHAR(MAX);
        DECLARE @TableName SYSNAME = ?;
        {declare}
        SET @SQLStatement = N'CREATE TABLE '+QUOTENAME(@TableName)+' ('+
        {syntax}
        {pk}
        +');'
        EXEC sp_executesql
        @SQLStatement,
        N'@TableName SYSNAME, {parameters}',
        @TableName=@TableName, {values};
        """

        # check inputs
        if sql_primary_key and primary_key_column is not None:
            raise ValueError('if sql_primary_key==True then primary_key_column has to be None')
        if isinstance(not_nullable, str):
            not_nullable = [not_nullable]
        if isinstance(primary_key_column, str):
            primary_key_column = [primary_key_column]

        # parse inputs
        column_names = list(columns.keys())
        alias_names = [str(x) for x in list(range(0,len(column_names)))]
        size, dtypes_sql = dynamic.column_spec(columns.values())
        size_vars = [alias_names[idx] if x is not None else None for idx,x in enumerate(size)]

        if primary_key_column is not None:
            alias_pk = [str(x) for x in list(range(0,len(primary_key_column)))]
        else:
            alias_pk = []

        # develop syntax for SQL variable declaration
        declare = list(zip(
            ["DECLARE @ColumnName_"+x+" SYSNAME = ?;" for x in alias_names],
            ["DECLARE @ColumnType_"+x+" SYSNAME = ?;" for x in alias_names],
            ["DECLARE @ColumnSize_"+x+" SYSNAME = ?;" if x is not None else "" for x in size_vars]
        ))
        declare = '\n'.join(['\n'.join(x) for x in declare])
        if primary_key_column is not None:
            declare += '\n'+'\n'.join(["DECLARE @PK_"+x+" SYSNAME = ?;" for x in alias_pk])

        # develop syntax for SQL table creation
        syntax = list(zip(
            ["QUOTENAME(@ColumnName_"+x+")" for x in alias_names],
            ["QUOTENAME(@ColumnType_"+x+")" for x in alias_names],
            ["@ColumnSize_"+x+"" if x is not None else "" for x in size_vars],
            ["'NOT NULL'" if x in not_nullable else "" for x in column_names]
        ))
        syntax = "+','+\n".join(
            ["+' '+".join([x for x in col if len(x)>0]) for col in syntax]
        )

        # primary key syntax
        pk = ""
        if sql_primary_key:
            syntax = "'_pk INT NOT NULL IDENTITY(1,1) PRIMARY KEY,'+\n"+syntax
        elif primary_key_column is not None:
            pk = "+','+".join(["QUOTENAME(@PK_"+x+")" for x in alias_pk])
            pk = "+\n',PRIMARY KEY ('+"+pk+"+')'"

        # develop syntax for sp_executesql parameters
        parameters = list(zip(
            ["@ColumnName_"+x+" SYSNAME" for x in alias_names],
            ["@ColumnType_"+x+" SYSNAME" for x in alias_names],
            ["@ColumnSize_"+x+" VARCHAR(MAX)" if x is not None else "" for x in size_vars]
        ))
        parameters = [", ".join([item for item in sublist if len(item)>0]) for sublist in parameters]
        parameters = ", ".join(parameters)
        if primary_key_column is not None:
            parameters += ", "+", ".join(["@PK_"+x+" SYSNAME" for x in alias_pk])

        # create input for sp_executesql SQL syntax
        values = list(zip(
            ["@ColumnName_"+x+""+"=@ColumnName_"+x+"" for x in alias_names],
            ["@ColumnType_"+x+""+"=@ColumnType_"+x+"" for x in alias_names],
            ["@ColumnSize_"+x+""+"=@ColumnSize_"+x+"" if x is not None else "" for x in size_vars]
        ))
        values = [", ".join([item for item in sublist if len(item)>0]) for sublist in values]
        values = ", ".join(values)
        if primary_key_column is not None:
            values += ", "+", ".join(["@PK_"+x+""+"=@PK_"+x+"" for x in alias_pk])

        # join components into final synax
        statement = statement.format(
            declare=declare,
            syntax=syntax,
            pk=pk,
            parameters=parameters,
            values=values
        )

        # create variables for execute method
        args = list(zip(
            [x for x in column_names],
            [x for x in dtypes_sql],
            [x for x in size]
        ))
        args = [item for sublist in args for item in sublist if item is not None]
        args = [table_name] + args
        if primary_key_column is not None:
            args += primary_key_column

        # execute statement
        cursor = self.connection.connection.cursor()
        cursor.execute(statement, args)


    def table_from_dataframe(self, table_name: str, dataframe: pd.DataFrame, primary_key : Literal[None,'sql','index','infer'] = None):
        """ Create SQL table by inferring SQL create table parameters from the contents of the DataFrame.

        Parameters
        ----------

        table_name (str) : name of table
        dataframe (pandas.DataFrame) : data used to create table
        primary_key (str, default = 'sql') : method of setting the table's primary key, see below for description of options

        primary_key = None : do not set a primary key
        primary_key = 'sql' : create an SQL managed auto-incrementing identity primary key column named '_pk'
        primary_key = 'index' : use the index of the dataframe and it's name, or '_index' if the index is not named
        primary_key = 'infer' : determine the column in the dataframe that best serves as a primary key and use it's name

        Returns
        -------

        dataframe (pandas.DataFrame) : data with a potentially different data type, depending on what SQL inferred

        Examples
        --------

        #### create table without a primary key

        df = create.table_from_dataframe('##DFNoPK', pd.DataFrame({"ColumnA": [1]}))

        #### create table with the dataframe's index as the primary key

        df = create.table_from_dataframe('##DFIndexPK', pd.DataFrame({"ColumnA": [1,2]}, index=['a','z']), primary_key='index')

        #### create an SQL identity primary key

        df = create.table_from_dataframe('##DFIdentityPK', pd.DataFrame({"ColumnA": [1,2]}), primary_key='sql')

        #### attempt to automatically find a primary key in the dataframe

        df = create.table_from_dataframe('##DFInferPK', pd.DataFrame({"ColumnA": [1,2], "ColumnB": ["a","b"]}), primary_key='infer')

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
            if not any(dataframe.index.names):
                dataframe.index.name = '_index'
            primary_key_column = list(dataframe.index.names)
            dataframe = dataframe.reset_index()
        elif primary_key == 'infer':
            sql_primary_key = False
            primary_key_column = None

        # infer SQL specifications from contents of dataframe
        dataframe, schema, not_nullable, pk = infer.sql(dataframe)
        _, dtypes = conversion.sql_spec(schema, dataframe)

        # infer primary key column after best fit data types have been determined
        if primary_key=='infer':
            primary_key_column = pk

        # create final SQL table
        self.table(table_name, dtypes, not_nullable=not_nullable, primary_key_column=primary_key_column, sql_primary_key=sql_primary_key)

        # issue message for derived table
        pk_name = primary_key_column
        if sql_primary_key:
            pk_name = '_pk (SQL managed int identity column)'
        elif primary_key=='index':
            pk_name = str(primary_key_column)+' (dataframe index)'
        elif primary_key_column is not None:
            pk_name = primary_key_column+' (dataframe column)'
        else:
            pk_name = 'None'
        msg = f'''
        Created table {table_name}
        Primary key: {pk_name}
        Non-null columns: {not_nullable}
        Data types: {dtypes}
        '''
        warnings.warn(msg, errors.SQLObjectAdjustment)

        # reset index after it was set as a column for table creation
        if primary_key=='index':
            dataframe = dataframe.set_index(keys=primary_key_column)
    
        return dataframe