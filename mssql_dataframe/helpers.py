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
    if isinstance(inputs, str):
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
        raise errors.GeneralError("GeneralError") from None
    
    if flatten:
        clean = clean[0]

    return clean


def where_clause(connection, where: str):
    ''' Safely format a where clause condition.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    where (str) : where conditions to apply

    Returns
    -------

    where_statement (str) : where statement containing parameters such as "...WHERE [username] = ?"
    where_args (list) : parameter values

    Example
    -------

    where_statement, where_args = where_clause(connection, 'ColumnA >5 AND ColumnB=2 and ColumnANDC IS NOT NULL')
    where_statement == 'WHERE [ColumnA] > ? AND [ColumnB] = ? and [ColumnANDC] IS NOT NULL'
    where_args == ['5','2']

    '''

    # regular expressions to parse where statement
    combine = r'\bAND\b|\bOR\b'
    comparison = ["=",">","<",">=","<=","<>","!=","!>","!<","IS NULL","IS NOT NULL"]
    comparison = r'('+'|'.join([x for x in comparison])+')'
    
    # split on AND/OR
    conditions = re.split(combine, where, flags=re.IGNORECASE)
    # split on comparison operator
    conditions = [re.split(comparison,x, flags=re.IGNORECASE) for x in conditions]
    if len(conditions)==1 and len(conditions[0])==1:
        raise errors.InvalidSyntax("invalid syntax for where = "+where)
    # form dict for each colum, while handling IS NULL/IS NOT NULL split
    conditions = [[y.strip() for y in x] for x in conditions]
    conditions = {x[0]:(x[1::] if len(x[2])>0 else [x[1]]) for x in conditions}

    # santize column names
    column_names =  safe_sql(connection, conditions.keys())
    column_names = dict(zip(conditions.keys(), column_names))
    conditions = dict((column_names[key], value) for (key, value) in conditions.items())
    conditions = conditions.items()

    # form SQL where statement
    where_statement = [x[0]+' '+x[1][0]+' ?' if len(x[1])>1 else x[0]+' '+x[1][0] for x in conditions]
    recombine = re.findall(combine, where, flags=re.IGNORECASE)+['']
    where_statement = list(zip(where_statement,recombine))
    where_statement = 'WHERE '+' '.join([x[0]+' '+x[1] for x in where_statement])
    where_statement = where_statement.strip()

    # form arguments, skipping IS NULL/IS NOT NULL
    where_args = {'param'+str(idx):x[1][1] for idx,x in enumerate(conditions) if len(x[1])>1}
    where_args = [x[1][1] for x in conditions if len(x[1])>1]

    return where_statement, where_args


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

    pattern = r"(\(\d+\)|\(\d.+\)|\(MAX\))"
    size = [re.findall(pattern, x) for x in columns]
    size = [x[0] if len(x)>0 else None for x in size]
    dtypes = [re.sub(pattern,'',var) for var in columns]

    if flatten:
        size = size[0]
        dtypes = dtypes[0]

    return size, dtypes


def infer_datatypes(connection, table_name: str, column_names: list):
    """ Dynamically determine SQL variable types by issuing a statement against an SQL table.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of table
    column_names (list|str) : columns to infer data types for

    Returns
    -------

    dtypes (dict) : keys = column name, values = data types and optionally size

    Dynamic SQL Sample
    ------------------

    """

    if isinstance(column_names, str):
        column_names = [column_names]

    # develop syntax for SQL variable declaration
    vars = list(zip(
        ["DECLARE @ColumnName_"+x+" SYSNAME = ?;" for x in column_names]
    ))

    vars = [
        "DECLARE @SQLStatement AS NVARCHAR(MAX);",
        "DECLARE @TableName SYSNAME = ?;",
    ] + ['\n'.join(x) for x in vars]

    vars = "\n".join(vars)

    # develop syntax for determine data types
    # # assign positional column names to avoid running raw input in SQL
    sql = list(zip(
        ["''Column"+str(idx)+"''" for idx,_ in enumerate(column_names)],
        ["+QUOTENAME(@ColumnName_"+x+")+" for x in column_names]
    ))
    sql = ",\n".join(["\t("+x[0]+", '"+x[1]+"')" for x in sql])

    sql = "CROSS APPLY (VALUES \n"+sql+" \n) v(ColumnName, _Column)"
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
        ELSE ''VARCHAR''
    END) AS type
    FROM '+QUOTENAME(@TableName)+'
    """+\
    sql+\
    """
    WHERE _Column IS NOT NULL
    GROUP BY ColumnName;'
    """  

    # develop syntax for sp_executesql parameters
    params = ["@ColumnName_"+x+" SYSNAME" for x in column_names]

    params = "N'@TableName SYSNAME, "+", ".join(params)+"'"

    # create input for sp_executesql SQL syntax
    load = ["@ColumnName_"+x+""+"=@ColumnName_"+x+"" for x in column_names]

    load = "@TableName=@TableName, "+", ".join(load)

    # join components into final synax
    statement = "\n".join([vars,sql,"EXEC sp_executesql \n @SQLStatement,",params+',',load+';'])

    # create variables for execute method
    args = [table_name] + [x for x in column_names]

    # execute statement
    dtypes = connection.cursor.execute(statement, *args).fetchall()
    dtypes = [x[1] for x in dtypes]
    dtypes = list(zip(column_names,dtypes))
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
         raise errors.TableDoesNotExist('{table_name} does not exist'.format(table_name=table_name)) from None
    
    schema = schema.set_index('column_name')
    schema['is_primary_key'] = schema['is_primary_key'].fillna(False)

    return schema