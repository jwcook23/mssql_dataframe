import re

import pandas as pd

import mssql_dataframe.create_sql.connect
from mssql_dataframe.create_sql import insert

class table():
    """ Creates SQL statements for interacting with table objects.

    Parameters
    ----------

    connection (mssql_dataframe.create_sql.connect) : connection for executing statement

    Properties
    -------

    self.cursor (mssql_dataframe.create_sql.connect.cursor) : for executing SQL statements
    
    self.insert (mssql_dataframe.create_sql.insert) : for inserting values into SQL tables 

    self.statement (str) : dynamic sql statment that is executed

    self.args (list) : arguments passed to sp_executesql

    """

    def __init__(self, connection : mssql_dataframe.create_sql.connect):
        
        self.cursor = connection.cursor
        self.insert = insert.insert(connection)
        self. statement = None
        self.args = None


    def create_table(self, name: str, columns: dict, primary_key: str = None , notnull: list = []) -> tuple:
        """Develop and execute SQL statement for table creation using sp_executesql stored procedure.
        Implements SQL "QUOTENAME" function and SQL "SYSNAME" datatype to prevent SQL injection 
        while allowing for variable table and column names.

        Parameters
        ----------

        name (str) : name of table to create

        columns (dict) : keys = column names, values = data types and optionally size

        primary_key (str, default=None) : column to set as the primary key

        notnull (list, default=[]) : list of columns to set as not null

        Returns
        -------

        None

        Examples
        -------

        columns = {'ColumnName': 'VARCHAR(100)'}

        pk = 'ColumnB'

        create_table(table='SQLTableName', columns=columns)

        """

        names = list(columns.keys())
        dtypes = columns.values()

        # extract SQL variable size
        pattern = r"(\(\d.+\)|\(MAX\))"
        size = [re.findall(pattern, x) for x in dtypes]
        size = [x[0] if len(x)>0 else "" for x in size]

        dtypes = [re.sub(pattern,'',var) for var in dtypes]

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
        sql = list(zip(
            ["QUOTENAME(@ColumnName_"+x+")" for x in names],
            ["QUOTENAME(@ColumnType_"+x+")" for x in names],
            ["@ColumnSize_"+x+"" if len(x)>0 else "" for x in size_vars],
            ["'NOT NULL'" if x in notnull else "" for x in names],
            ["'PRIMARY KEY'" if primary_key is not None and x in primary_key else "" for x in names],
        ))

        sql = "+','+\n".join(
            ["+' '+".join([x for x in col if len(x)>0]) for col in sql]
        )

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
        self.statement = "\n".join([vars,sql,"EXEC sp_executesql \n @SQLStatement,",params+',',load+';'])

        # create variables for execute method
        args = list(zip(
            [x for x in names],
            [x for x in dtypes],
            [x for x in size]
        ))

        args = [item for sublist in args for item in sublist if len(item)>0]

        self.args = [name] + args

        # execute statement
        self.cursor.execute(self.statement, *self.args)


    def from_dataframe(self, name: str, dataframe: pd.DataFrame, primary_key : str = 'auto', row_count: int = 1000) -> tuple:
        """ Create SQL table using a DataFrame. Initally assumes a default max size
        data type then use SQL to automatically infer the best data type. Creates a table
        with either no primary key, an auto incrementing SQL managed primary key,
        or a primary key based on the DataFrame's index.

        Parameters
        ----------

        name (str) : name of table

        dataframe (DataFrame) : dataframe to determine datatypes of columns

        primary_key (str, default = 'auto') : 'auto' for SQL managed, 'index' for DataFrame's index, or None

        row_count (int, default = 1000) : number of rows for determining data types

        Returns
        -------

        None

        """

        # assume initial default data type
        columns = {x:'NVARCHAR(MAX)' for x in dataframe.columns}

        # dataframe index as SQL primary key
        if primary_key == 'auto':
            raise NotImplementedError('SQL managed primary key not implemented')
        elif primary_key == 'index':
            # assume 900-byte limit of SQL Server for index keys
            columns[dataframe.index.name] = 'NVARCHAR(450)'

        if primary_key is None:
            pk = None
        else:
            pk = dataframe.index.name
            dataframe.reset_index(inplace=True)

        # notnull columns
        notnull = list(dataframe.columns[dataframe.notna().all()])

        # create temp table and insert sample for determining data types
        name_temp = "##dtype_"+name

        self.create_table(name_temp, columns, pk, notnull)

        subset = dataframe.loc[0:row_count, :]
        self.insert.insert_data(name=name_temp, dataframe=subset)

        # determine best datatype in SQL
        dtypes = self.__infer_datatypes(name=name_temp, columns=list(dataframe.columns))

        # create final SQL table
        self.create_table(name, dtypes, pk, notnull)


    def __infer_datatypes(self, name: str, columns: list) -> tuple:
        """ Dynamically determine SQL variable types by issuing a statement against an SQL table.

        Parameters
        ----------

        name (str) : name of table

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
        
        sql = """
        SET @SQLStatement = N'
        SELECT ColumnName,
        (CASE 
            WHEN count(try_convert(TINYINT, _Column)) = count(_Column) THEN ''TINYINT''
            WHEN count(try_convert(INT, _Column)) = count(_Column) THEN ''INT''
            WHEN count(try_convert(BIGINT, _Column)) = count(_Column) THEN ''BIGINT''
            WHEN count(try_convert(DATE, _Column)) = count(_Column) THEN ''DATE''
            WHEN count(try_convert(TIME, _Column)) = count(_Column) THEN ''TIME''
            WHEN count(try_convert(DATETIME, _Column)) = count(_Column) THEN ''DATETIME''
            WHEN count(try_convert(NUMERIC(20, 4), _Column)) = count(_Column) 
                AND sum(CASE WHEN _Column LIKE ''%._____'' THEN 1 ELSE 0 END) = 0
                THEN ''NUMERIC(20, 4)''
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
        self.statement = "\n".join([vars,sql,"EXEC sp_executesql \n @SQLStatement,",params+',',load+';'])

        # create variables for execute method
        self.args = [name] + [x for x in columns]

        # execute statement
        dtypes = self.cursor.execute(self.statement, *self.args).fetchall()
        dtypes = [x[1] for x in dtypes]
        dtypes = list(zip(columns,dtypes))
        dtypes = {x[0]:x[1] for x in dtypes}

        return dtypes