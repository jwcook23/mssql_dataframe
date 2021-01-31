""" Creates SQL query statements and variabes for execute method.
"""

import re

import pandas as pd
import numpy as np


def create_table(name: str, columns: dict, primary_key: str = "" , notnull: list = []):
    """Develop SQL statement for table creation using sp_executesql stored procedure.
    Implements SQL "QUOTENAME" function and SQL "sysname" datatype to prevent SQL 
    injection while allowing for variable table and column names.

    Parameters
    ----------

    name (str) : name of table to create

    columns (dict) : keys = column names, values = data types and optionally size

    primary_key (str, default=None) : column to set as the primary key

    notnull (list, default=[]) : list of columns to set as not null

    Returns
    -------

    statement (str) : query statement string to pass to execute method

    args (list) : *args of column names and data types to pass to execute method

    Examples
    -------

    columns = {'ColumnA': 'VARCHAR(100)', 'ColumnB': 'INT'}

    pk = 'ColumnB'

    statement, args = create_table(table='SQLTableName', columns=columns)

    cursor.execute(statement, *args)

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

    return statement, args


def insert_data(name: str, dataframe: pd.DataFrame):
    """Develop SQL statement for inserting data.

    Parameters
    ----------

    name (str) : name of table to create

    dataframe(pd.DataFrame): tabular data to insert

    Returns
    -------
    
    statement (str) : query statement string to pass to execute method

    values (list) : values to pass to execute method

    Examples
    --------

    data = pd.DataFrame({'ColumnA': [1, 2, 3]})

    statement, values = execute_statement.insert_data('TableName', data)

    cursor.executemany(statement, values)

    """    

    # interpret any kind of missing values as NULL in SQL
    dataframe = dataframe.fillna(np.nan).replace([np.nan], [None])

    # extract values to insert into a list of lists
    values = dataframe.values.tolist()

    # form parameterized statement
    columns = ", ".join(dataframe.columns)

    params = '('+', '.join(['?']*len(dataframe.columns))+')'
    
    statement = "INSERT INTO "+name+" ("+columns+") VALUES "+params

    return statement, values