import re

import pandas as pd

from mssql_dataframe import errors


def safe_sql(connection, inputs):
    ''' Sanitize a list of string inputs into safe object names.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    inputs (list|str) : list of strings to sanitize

    Returns
    -------

    clean (tuple) : santized strings

    '''
    
    flatten = False
    if isinstance(inputs,str):
        flatten = True
        inputs = [inputs]
    elif not isinstance(inputs, list):
        inputs = list(inputs)

    statement = "SELECT {syntax}"
    syntax = ", ".join(["QUOTENAME(?)"]*len(inputs))
    statement = statement.format(syntax=syntax)
    
    try: 
        clean = connection.cursor.execute(statement, inputs).fetchone()
    except:
        raise errors.GeneralError("GeneralError")
    
    if flatten:
        clean = clean[0]

    return clean


def column_spec(columns: list):
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
    size = [x[0] if len(x)>0 else None for x in size]
    dtypes = [re.sub(pattern,'',var) for var in columns]

    if flatten:
        size = size[0]
        dtypes = dtypes[0]

    return size, dtypes


def infer_datatypes(connection, table_name: str, columns: list):
    """ Dynamically determine SQL variable types by issuing a statement against an SQL table.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of table
    columns (list) : columns to infer data types for

    Returns
    -------

    dtypes (dict) : keys = column name, values = data types and optionally size

    Dynamic SQL Sample
    ------------------

    """

    cursor = connection.cursor()

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
    END) AS type
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
    dtypes = cursor.execute(statement, *args).fetchall()
    dtypes = [x[1] for x in dtypes]
    dtypes = list(zip(columns,dtypes))
    dtypes = {x[0]:x[1] for x in dtypes}

    return dtypes


def read_query(connection, statement: str, arguments: list = None) -> pd.DataFrame:
    
    if arguments is None:
         dataframe = connection.cursor.execute(statement)
    else:
        dataframe = connection.cursor.execute(statement, *arguments)
    dataframe = dataframe.fetchall()
    dataframe = [list(x) for x in dataframe]

    columns = [col[0] for col in connection.cursor.description]
    dataframe = pd.DataFrame(dataframe, columns=columns)

    return dataframe

def get_schema(connection, table_name: str):
    ''' Get SQL schema of a table.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of table to retrieve schema of

    Returns
    -------
    schema (pandas.DataFrame) : schema for each column in the table

    '''

    table_name = safe_sql(connection, table_name)

    statement = """
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
    """

    statement = statement.format(table_name=table_name)

    schema = read_query(connection, statement)
    if len(schema)==0:
         raise errors.TableDoesNotExist('{table_name} does not exist'.format(table_name=table_name))
    
    schema = schema.set_index('column_name')
    schema['is_primary_key'] = schema['is_primary_key'].fillna(False)

    return schema