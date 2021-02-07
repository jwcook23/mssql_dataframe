import re

import pandas as pd

import mssql_dataframe.create_sql.connection

class table():
    """ Creates SQL statements for interacting with table objects.

    Parameters
    ----------

    connection (mssql_dataframe.create_sql.connection) : connection to execute statement

    Returns
    -------

    None

    """

    def __init__(self, connection : mssql_dataframe.create_sql.connection):
        
        self.connection = connection


    def create_table(self, name: str, columns: dict, primary_key: str = "" , notnull: list = []):
        """Develop SQL and execute statement for table creation using sp_executesql stored procedure.
        Implements SQL "QUOTENAME" function and SQL "sysname" datatype to prevent SQL injection 
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

        columns = {'ColumnA': 'VARCHAR(100)', 'ColumnB': 'INT'}

        pk = 'ColumnB'

        create_table(table='SQLTableName', columns=columns)

        """

        names = list(columns.keys())
        dtypes = columns.values()

        # extract SQL variable size
        pattern = r"(\(\d.+\))"
        size = [re.findall(pattern, x) for x in dtypes]
        size = [x[0] if len(x)>0 else "" for x in size]

        dtypes = [re.sub(pattern,'',var) for var in dtypes]

        size_vars = [idx if len(x)>0 else None for idx,x in enumerate(size)]
        size_vars = [names[x] if x is not None else "" for x in size_vars]

        # develop syntax for SQL variable declaration
        vars = list(zip(
            ["DECLARE @_"+x+"_name sysname = ?;" for x in names],
            ["DECLARE @_"+x+"_type sysname = ?;" for x in names],
            ["DECLARE @_"+x+"_size sysname = ?;" if len(x)>0 else "" for x in size_vars]
        ))

        vars = [
            "DECLARE @sql AS NVARCHAR(MAX);",
            "DECLARE @_table sysname = ?;",
        ] + ['\n'.join(x) for x in vars]

        vars = "\n".join(vars)

        # develop syntax for SQL table creation
        table = list(zip(
            ["QUOTENAME(@_"+x+"_name)" for x in names],
            ["QUOTENAME(@_"+x+"_type)" for x in names],
            ["@_"+x+"_size" if len(x)>0 else "" for x in size_vars],
            ["'NOT NULL'" if x in notnull else "" for x in names],
            ["'PRIMARY KEY'" if x in primary_key else "" for x in names],
        ))

        table = "+','+\n".join(
            ["+' '+".join([x for x in col if len(x)>0]) for col in table]
        )

        table = "SET @sql = N'CREATE TABLE '+QUOTENAME(@_table)+' ('+\n"+table+"+\n');'"

        # develop syntax for sp_executesql parameters
        params = list(zip(
            ["@_"+x+"_name sysname" for x in names],
            ["@_"+x+"_type sysname" for x in names],
            ["@_"+x+"_size VARCHAR(MAX)" if len(x)>0 else "" for x in size_vars]
        ))

        params = [", ".join([item for item in sublist if len(item)>0]) for sublist in params]
        params = "N'@_table sysname, "+", ".join(params)+"'"

        # create input for sp_executesql SQL syntax
        load = list(zip(
            ["@_"+x+"_name"+"=@_"+x+"_name" for x in names],
            ["@_"+x+"_type"+"=@_"+x+"_type" for x in names],
            ["@_"+x+"_size"+"=@_"+x+"_size" if len(x)>0 else "" for x in size_vars]
        ))

        load = [", ".join([item for item in sublist if len(item)>0]) for sublist in load]

        load = "@_table=@_table, "+", ".join(load)

        # join components into final synax
        statement = "\n".join([vars,table,"EXEC sp_executesql @sql,",params+',',load+';'])

        # create variables for execute method
        args = list(zip(
            [x for x in names],
            [x for x in dtypes],
            [x for x in size]
        ))

        args = [item for sublist in args for item in sublist if len(item)>0]

        args = [name] + args

        
        # execute statement
        self.connection.cursor.execute(statement, *args)


    def from_dataframe(self, name: str, dataframe: pd.DataFrame, row_count: int = 1000):
        """ Create database table using a dataframe and attempt to automatically infer
        the best SQL data type. If the datafarme index is named, it is used to create
        the primary key. Otherwise an autoincrementing BIGINT primary key is created named "_pk".

        Parameters
        ----------

        name (str) : name of table

        dataframe (DataFrame) : dataframe to determine datatypes of columns

        row_count (int, default = 1000) : number of rows for determining data types

        Returns
        -------

        None

        """

        # assume initial default data type
        columns = {x:'VARCHAR' for x in dataframe.columns}

        # dataframe index as SQL primary key
        if dataframe.index.name is None:
            dataframe.index.name = '_pk'
            columns['_pk'] = 'BIGINT'
        else:
            columns[dataframe.index.name] = 'VARCHAR'

        pk = dataframe.index.name
        dataframe.reset_index(inplace=True)

        # notnull columns
        notnull = list(dataframe.columns[dataframe.notna().all()])

        # create table for determining data types
        name_temp = "##dtype_"+name

        self.create_table(name_temp, columns, pk, notnull)

        dataframe.loc[1:row_count, :]