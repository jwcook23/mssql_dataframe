import re
from typing import Literal
from numpy.core.fromnumeric import size

import pandas as pd
import numpy as np

import mssql_dataframe.connect
from mssql_dataframe import insert


class table():
    """ Create or modify SQL tables and table properties.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement

    """

    def __init__(self, connection : mssql_dataframe.connect):

        self.cursor = connection.cursor
        self.insert = insert.insert(connection)


    def get_schema(self, table_name):
        ''' Get SQL schema of a table.

        Parameters
        ----------
        table_name (str) : name of table to retrieve schema of

        Returns
        -------
        schema (pandas.DataFrame) : schema for each column in the table

        '''

        statement = '''
        SELECT
            sys.columns.name AS column_name,
            TYPE_NAME(SYSTEM_TYPE_ID) AS data_type, 
            sys.columns.max_length, 
            sys.columns.precision, 
            sys.columns.scale, 
            sys.columns.is_nullable, 
            sys.columns.is_identity,
            sys.indexes.is_primary_key
        FROM sys.columns
        LEFT JOIN sys.index_columns
            ON sys.index_columns.object_id = sys.columns.object_id 
            AND sys.index_columns.column_id = sys.columns.column_id
        LEFT JOIN sys.indexes
            ON sys.indexes.object_id = sys.index_columns.object_id 
            AND sys.indexes.index_id = sys.index_columns.index_id
        WHERE sys.columns.object_ID = OBJECT_ID('{table_name}')
        '''
        statement = statement.format(table_name=table_name)

        schema = self.cursor.execute(statement).fetchall()
        if len(schema)==0:
            raise TableDoesNotExist('{table_name} does not exist'.format(table_name=table_name))
        schema = [list(x) for x in schema]

        columns = [col[0] for col in self.cursor.description]
        schema = pd.DataFrame(schema, columns=columns).set_index('column_name')
        schema['is_primary_key'] = schema['is_primary_key'].fillna(False)

        return schema


    def create_table(self, table_name: str, columns: dict, not_null: list = [],
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

        sp_executesql statement
        -----------------------

        DECLARE @SQLStatement AS NVARCHAR(MAX);
        DECLARE @TableName SYSNAME = ?;
        DECLARE @ColumnName_A SYSNAME = ?;
        DECLARE @ColumnType_A SYSNAME = ?;
        DECLARE @ColumnSize_A SYSNAME = ?;
        SET @SQLStatement = N'CREATE TABLE '+QUOTENAME(@TableName)+' ('+
        QUOTENAME(@ColumnName_A)+' '+QUOTENAME(@ColumnType_A)+' '+@ColumnSize_A+
        ');'
        EXEC sp_executesql 
        @SQLStatement,
        N'@TableName SYSNAME, @ColumnName_A SYSNAME, @ColumnType_A SYSNAME, @ColumnSize_A VARCHAR(MAX)',
        @TableName=@TableName, @ColumnName_A=@ColumnName_A, @ColumnType_A=@ColumnType_A, @ColumnSize_A=@ColumnSize_A;


        sp_executesql parameters
        ------------------------

        ['##SingleColumn', 'A', 'VARCHAR', '(100)']

        """

        names = list(columns.keys())
        # dtypes = columns.values()

        # extract SQL variable size
        size, dtypes = self.__variable_spec(columns.values())
        # pattern = r"(\(\d.+\)|\(MAX\))"
        # size = [re.findall(pattern, x) for x in dtypes]
        # size = [x[0] if len(x)>0 else "" for x in size]

        # dtypes = [re.sub(pattern,'',var) for var in dtypes]

        size_vars = [idx if len(x)>0 else None for idx,x in enumerate(size)]
        size_vars = [names[x] if x is not None else "" for x in size_vars]

        # develop syntax for SQL variable declaration
        vars = list(zip(
            ["DECLARE @ColumnName_"+x+" SYSNAME = ?;" for x in names],
            ["DECLARE @ColumnType_"+x+" SYSNAME = ?;" for x in names],
            ["DECLARE @ColumnSize_"+x+" SYSNAME = ?;" if len(x)>0 else "" for x in size_vars]
        ))

        vars = [
            "DECLARE @SQLStatement AS NVARCHAR(MAX);",
            "DECLARE @TableName SYSNAME = ?;",
        ] + ['\n'.join(x) for x in vars]

        vars = "\n".join(vars)

        # develop syntax for SQL table creation
        if sql_primary_key and primary_key_column is not None:
            raise ValueError('if sql_primary_key==True then primary_key_column has to be None')

        sql = list(zip(
            ["QUOTENAME(@ColumnName_"+x+")" for x in names],
            ["QUOTENAME(@ColumnType_"+x+")" for x in names],
            ["@ColumnSize_"+x+"" if len(x)>0 else "" for x in size_vars],
            ["'NOT NULL'" if x in not_null else "" for x in names],
            ["'PRIMARY KEY'" if x==primary_key_column else "" for x in names]
        ))

        sql = "+','+\n".join(
            ["+' '+".join([x for x in col if len(x)>0]) for col in sql]
        )

        if sql_primary_key:
            sql = "'_pk INT NOT NULL IDENTITY(1,1) PRIMARY KEY,'+\n"+sql

        sql = "SET @SQLStatement = N'CREATE TABLE '+QUOTENAME(@TableName)+' ('+\n"+sql+"+\n');'"

        # develop syntax for sp_executesql parameters
        params = list(zip(
            ["@ColumnName_"+x+" SYSNAME" for x in names],
            ["@ColumnType_"+x+" SYSNAME" for x in names],
            ["@ColumnSize_"+x+" VARCHAR(MAX)" if len(x)>0 else "" for x in size_vars]
        ))

        params = [", ".join([item for item in sublist if len(item)>0]) for sublist in params]
        params = "N'@TableName SYSNAME, "+", ".join(params)+"'"

        # create input for sp_executesql SQL syntax
        load = list(zip(
            ["@ColumnName_"+x+""+"=@ColumnName_"+x+"" for x in names],
            ["@ColumnType_"+x+""+"=@ColumnType_"+x+"" for x in names],
            ["@ColumnSize_"+x+""+"=@ColumnSize_"+x+"" if len(x)>0 else "" for x in size_vars]
        ))

        load = [", ".join([item for item in sublist if len(item)>0]) for sublist in load]

        load = "@TableName=@TableName, "+", ".join(load)

        # join components into final synax
        statement = "\n".join([vars,sql,"EXEC sp_executesql \n @SQLStatement,",params+',',load+';'])

        # create variables for execute method
        args = list(zip(
            [x for x in names],
            [x for x in dtypes],
            [x for x in size]
        ))

        args = [item for sublist in args for item in sublist if len(item)>0]

        args = [table_name] + args

        # execute statement
        self.cursor.execute(statement, *args)


    def from_dataframe(self, table_name: str, dataframe: pd.DataFrame, primary_key : Literal[None,'sql','index','infer'] = None, 
    row_count: int = 1000):
        """ Create SQL table by inferring SQL create table parameters from the contents of the DataFrame. 
        After table creation, the DataFrame values are inserted into the table.

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

        # assume initial default data type
        columns = {x:'NVARCHAR(MAX)' for x in dataframe.columns}

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
            # # use the max allowed size for a primary key
            columns[dataframe.index.name] = 'NVARCHAR(450)'
            primary_key_column = dataframe.index.name
            dataframe = dataframe.reset_index()
        elif primary_key == 'infer':
            sql_primary_key = False
            primary_key_column = None

        # not_null columns
        not_null = list(dataframe.columns[dataframe.notna().all()])

        # create temp table to determine data types
        name_temp = "##DataType_"+table_name
        self.create_table(name_temp, columns, not_null=not_null, primary_key_column=primary_key_column, sql_primary_key=None)

        # insert data into temp table to determine datatype
        subset = dataframe.loc[0:row_count, :]
        datetimes = subset.select_dtypes('datetime').columns
        numeric = subset.select_dtypes(include=np.number).columns
        subset = subset.astype('str')
        for col in subset:
            subset[col] = subset[col].str.strip()
        # # truncate datetimes to 3 decimal places
        subset[datetimes] = subset[datetimes].replace(r'(?<=\.\d{3})\d+','', regex=True)
        # # remove zero decimal places from numeric values
        subset[numeric] = subset[numeric].replace(r'\.0+','', regex=True)
        # # treat empty like as None (NULL in SQL)
        subset = subset.replace({'': None, 'None': None, 'nan': None, 'NaT': None})
        # insert subset of data then use SQL to determine SQL data type
        self.insert.insert_data(table_name=name_temp, dataframe=subset)
        dtypes = self.__infer_datatypes(table_name=name_temp, columns=list(dataframe.columns))

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

        # create final SQL table then insert data
        self.create_table(table_name, dtypes, not_null=not_null, primary_key_column=primary_key_column, sql_primary_key=sql_primary_key)
        self.insert.insert_data(table_name, dataframe)


    def modify_column(self, table_name: str, modify: Literal['add','alter','drop'], column_name: str, data_type: str = None, not_null=False):
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

        sp_executesql statement
        -----------------------

        DECLARE @SQLStatement AS NVARCHAR(MAX);
        DECLARE @TableName SYSNAME = ?;
        DECLARE @ColumnName_A SYSNAME = ?;
        DECLARE @ColumnType_A SYSNAME = ?;
        DECLARE @ColumnSize_A SYSNAME = ?;
        SET @SQLStatement = N'ALTER TABLE '+QUOTENAME(@TableName)
        ALTER COLUMN QUOTENAME(@ColumnName_A)+' '+QUOTENAME(@ColumnType_A)+' '+@ColumnSize_A+
        ';'
        EXEC sp_executesql 
        @SQLStatement,
        N'@TableName SYSNAME, @ColumnName_A SYSNAME, @ColumnType_A SYSNAME, @ColumnSize_A VARCHAR(MAX)',
        @TableName=@TableName, @ColumnName_A=@ColumnName_A, @ColumnType_A=@ColumnType_A, @ColumnSize_A=@ColumnSize_A;


        sp_executesql parameters
        ------------------------

        ['##SingleColumn', 'A', 'VARCHAR', '(100)']

        """
        
        statement = '''
            DECLARE @SQLStatement AS NVARCHAR(MAX);
            DECLARE @TableName SYSNAME = ?;
            DECLARE @ColumnName SYSNAME = ?;
            {type_declare}
            {size_declare}

            SET @SQLStatement = 
                N'ALTER TABLE '+QUOTENAME(@TableName)+
                {modify_statement}+';'

            EXEC sp_executesql 
                @SQLStatement,
                N'@TableName SYSNAME, @ColumnName SYSNAME {type_parameter} {size_parameter}',
                @TableName=@TableName, @ColumnName=@ColumnName {type_value} {size_value};
        '''

        args = [table_name, column_name]
        if modify=='drop':
            type_declare = ""
            size_declare = ""
            modify_statement = "' DROP COLUMN '+QUOTENAME(@ColumnName)"
            type_parameter = ""
            size_parameter = ""
            type_value = ""
            size_value = ""
        elif modify=='add':
            type_declare = "DECLARE @ColumnType SYSNAME = ?;"
            size_declare = "DECLARE @ColumnSize SYSNAME = ?;"
            modify_statement = "' ADD '+QUOTENAME(@ColumnName)+' '+QUOTENAME(@ColumnType)+' '+@ColumnSize"
            type_parameter = ",@ColumnType SYSNAME"
            size_parameter = ",@ColumnSize VARCHAR(MAX)"
            type_value = ",@ColumnType=@ColumnType"
            size_value = ",@ColumnSize=@ColumnSize"
            args += ["VARCHAR","(20)"]
        

        statement = statement.format(
            type_declare=type_declare, size_declare=size_declare,
            modify_statement=modify_statement,
            type_parameter=type_parameter, size_parameter=size_parameter,
            type_value=type_value, size_value=size_value
        )


        self.cursor.execute(statement, *args)


    def __variable_spec(columns: list):
        ''' Extract SQL data type, size, and precision from list of strings.

        Parameters
        ----------
        
        columns (list|str) : strings to extract SQL specifications from

        Returns
        -------

        size (list|str)

        dtypes (list|str)

        '''

        flatten = False
        if isinstance(columns,str):
            columns = [columns]
            flatten = True

        pattern = r"(\(\d.+\)|\(MAX\))"
        size = [re.findall(pattern, x) for x in columns]
        size = [x[0] if len(x)>0 else "" for x in size]
        dtypes = [re.sub(pattern,'',var) for var in columns]

        if flatten:
            size = size[0]
            dtypes = dtypes[0]

        return size, dtypes

    def __infer_datatypes(self, table_name: str, columns: list):
        """ Dynamically determine SQL variable types by issuing a statement against an SQL table.

        Parameters
        ----------

        table_name (str) : name of table
        columns (list) : columns to infer data types for

        Returns
        -------

        dtypes (dict) : keys = column name, values = data types and optionally size

        Dynamic SQL Sample
        ------------------

        """

        # develop syntax for SQL variable declaration
        vars = list(zip(
            ["DECLARE @ColumnName_"+x+" SYSNAME = ?;" for x in columns]
        ))

        vars = [
            "DECLARE @SQLStatement AS NVARCHAR(MAX);",
            "DECLARE @TableName SYSNAME = ?;",
        ] + ['\n'.join(x) for x in vars]

        vars = "\n".join(vars)

        # develop syntax for determine data types
        # # assign positional column names to avoid running raw input in SQL
        sql = list(zip(
            ["''Column"+str(idx)+"''" for idx,_ in enumerate(columns)],
            ["+QUOTENAME(@ColumnName_"+x+")+" for x in columns]
        ))
        sql = ",\n".join(["\t("+x[0]+", '"+x[1]+"')" for x in sql])

        sql = "CROSS APPLY (VALUES \n"+sql+" \n) v(ColumnName, _Column)"
        # TODO: use SQL function DATALENGTH instead of assuming VARCHAR(255): maybe in same call?
        sql = """
        SET @SQLStatement = N'
        SELECT ColumnName,
        (CASE 
            WHEN count(try_convert(TINYINT, _Column)) = count(_Column) THEN ''TINYINT''
            WHEN count(try_convert(SMALLINT, _Column)) = count(_Column) THEN ''SMALLINT''
            WHEN count(try_convert(INT, _Column)) = count(_Column) THEN ''INT''
            WHEN count(try_convert(BIGINT, _Column)) = count(_Column) THEN ''BIGINT''
            WHEN count(try_convert(TIME, _Column)) = count(_Column) 
                AND SUM(CASE WHEN try_convert(DATE, _Column) = ''1900-01-01'' THEN 0 ELSE 1 END) = 0
                THEN ''TIME''
            WHEN count(try_convert(DATETIME, _Column)) = count(_Column) THEN ''DATETIME''
            WHEN count(try_convert(FLOAT, _Column)) = count(_Column) THEN ''FLOAT''
            ELSE ''VARCHAR(255)''
        END) AS column_type
        FROM '+QUOTENAME(@TableName)+'
        """+\
        sql+\
        """
        WHERE _Column IS NOT NULL
        GROUP BY ColumnName;'
        """  

        # develop syntax for sp_executesql parameters
        params = ["@ColumnName_"+x+" SYSNAME" for x in columns]

        params = "N'@TableName SYSNAME, "+", ".join(params)+"'"

        # create input for sp_executesql SQL syntax
        load = ["@ColumnName_"+x+""+"=@ColumnName_"+x+"" for x in columns]

        load = "@TableName=@TableName, "+", ".join(load)

        # join components into final synax
        statement = "\n".join([vars,sql,"EXEC sp_executesql \n @SQLStatement,",params+',',load+';'])

        # create variables for execute method
        args = [table_name] + [x for x in columns]

        # execute statement
        dtypes = self.cursor.execute(statement, *args).fetchall()
        dtypes = [x[1] for x in dtypes]
        dtypes = list(zip(columns,dtypes))
        dtypes = {x[0]:x[1] for x in dtypes}

        return dtypes


class TableDoesNotExist(Exception):
    '''Exception for SQL table that does not exist.'''
    pass