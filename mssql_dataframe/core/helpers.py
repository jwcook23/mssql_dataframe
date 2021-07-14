import re
import warnings

import pandas as pd
import numpy as np
import pyodbc

from mssql_dataframe.core import errors, create, write, modify


def execute(connection, statement:str, args:list=None):
    '''Execute an SQL statement prevent exposing any errors.
    
    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    statement (str) : statement to execute
    args (list, default=None) : optional placeholder arguments to pass when executing statement

    Returns
    -------

    None
    
    '''

    try:
        if args is None:
            connection.cursor.execute(statement)
        else:
            connection.cursor.execute(statement, *args)
    except:
        raise errors.SQLGeneral("Generic SQL error in helpers.execute") from None
    

def read_query(connection, statement: str, args: list = None) -> pd.DataFrame:
    ''' Read SQL query and return results in a dataframe. Skip errors due to undefined ODBC SQL data types.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    statement (str) : statement to execute
    args (list, default=None) : arguments to pass to execute

    Returns
    -------
    
    dataframe (pandas.DataFrame) :
    '''

    # execute the statement before getting results
    execute(connection, statement, args)

    # return result as string if SQL ODBC data type is not defined
    undefined_type = None
    default_index = []
    while undefined_type is None or len(undefined_type)>0:
        try:
            dataframe = connection.cursor.fetchall()
            undefined_type = []
        except pyodbc.ProgrammingError as error:
            undefined_type = re.findall(r'ODBC SQL type (-?\d+) is not yet supported.*',error.args[0])
            if len(undefined_type)>0:
                # set undefined type default conversion as str
                default_index += [int(re.findall(r'.*column-index=(\d+).*',error.args[0])[0])]
                connection.connection.add_output_converter(int(undefined_type[0]), str)
                execute(connection, statement, args)
            else:
                raise errors.SQLGeneral("Generic SQL error in helpers.read_query") from None
        except:
            raise errors.SQLGeneral("Generic SQL error in helpers.read_query") from None

    # form dataframe with column names
    dataframe = [list(x) for x in dataframe]
    columns = [col[0] for col in connection.cursor.description]
    dataframe = pd.DataFrame(dataframe, columns=columns)

    # issue warning for undefined SQL ODBC data types
    if len(default_index)>0:
        warnings.warn("Undefined Python data type generically inferred as strings for columns: "+str(list(dataframe.columns[default_index])))

    return dataframe


def safe_sql(connection, inputs):
    ''' Sanitize a list of string inputs into safe object names.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    inputs (list|str) : list of strings to sanitize

    Returns
    -------

    clean (list|str) : santized strings

    '''
    
    # handle string and other collection type inputs
    flatten = False
    if isinstance(inputs, str):
        flatten = True
        inputs = [inputs]
    elif not isinstance(inputs, list):
        inputs = list(inputs)

    # check for possible schema specification
    # # flatten each list and combine with the char(255) delimiter that will very likely never occur
    schema = [re.findall(r'\.+',x) for x in inputs]
    schema = [x+[chr(255)] for x in schema]
    schema = [item for sublist in schema for item in sublist]
    inputs = [re.split(r'\.+',x) for x in inputs]
    inputs = [item for sublist in inputs for item in sublist]

    # use SQL to construct a valid SQL delimited identifier
    statement = "SELECT {syntax}"
    syntax = ", ".join(["QUOTENAME(?)"]*len(inputs))
    statement = statement.format(syntax=syntax)
    execute(connection, statement, inputs)
    clean = connection.cursor.fetchone()
    # a value is too long and returns None, so raise an exception
    if len([x for x in clean if x is None])>0:
        raise errors.SQLInvalidLengthObjectName("SQL object name is too long.") from None
    
    # reconstruct possible schema specification
    clean = list(zip(clean,schema))
    clean = [item for sublist in clean for item in sublist]
    clean = "".join(clean[0:-1]).split(chr(255))

    # return string if string was input
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
        raise errors.SQLInvalidSyntax("invalid syntax for where = "+where)
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


def infer_datatypes(connection, table_name: str, dataframe: pd.DataFrame, row_count: int = 1000):
    """ Dynamically determine SQL variable types by issuing a statement against a temporary SQL table.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of temporary table to create
    dataframe (pandas.DataFrame) : data that needs data type inferred
    row_count (int, default = 1000) : number of rows for determining data types

    Returns
    -------

    dtypes (dict) : keys = column name, values = data types and optionally size


    """
    # create temporary table
    columns = {x:'NVARCHAR(MAX)' for x in dataframe.columns}
    bld = create.create(connection)
    bld.table(table_name, columns)
    
    # select random subset of data, ensuring the maximum values are included
    subset = dataframe.sample(n=min([row_count,len(dataframe)]))
    strings = dataframe.columns[dataframe.apply(lambda x: hasattr(x,'str'))]
    datetimes = dataframe.select_dtypes('datetime').columns
    numeric = dataframe.select_dtypes(include=np.number).columns
    include = pd.Series(dtype='int64')
    if len(datetimes)>0 or len(numeric)>0:
        include.append(dataframe[list(datetimes)+list(numeric)].idxmax())
    if len(strings)>0:
        include = include.append(dataframe[strings].apply(lambda x: x.str.len()).idxmax())
    include = include.drop_duplicates()
    subset = subset.append(dataframe.loc[include[~include.isin(subset.index)]])

    # insert subset of data into temporary table
    subset = subset.astype('str')
    for col in subset:
        subset[col] = subset[col].str.strip()
    # # truncate datetimes to 3 decimal places
    subset[datetimes] = subset[datetimes].replace(r'(?<=\.\d{3})\d+','', regex=True)
    # # remove zero decimal places from numeric values
    subset[numeric] = subset[numeric].replace(r'\.0+','', regex=True)
    # # treat empty like as None (NULL in SQL)
    subset = subset.replace({'': None, 'None': None, 'nan': None, 'NaT': None, '<NA>': None})
    # insert subset of data then use SQL to determine SQL data type
    wrt = write.write(connection, adjust_sql_objects=False)
    wrt.insert(table_name, dataframe=subset)

    statement = """
    DECLARE @SQLStatement AS NVARCHAR(MAX);
    DECLARE @TableName SYSNAME = ?;
    {declare}
    SET @SQLStatement = N'
        SELECT ColumnName,
        (CASE 
            WHEN count(try_convert(BIT, _Column)) = count(_Column) 
                AND MAX(_Column)=1 AND count(_Column)>2 THEN ''bit''
            WHEN count(try_convert(TINYINT, _Column)) = count(_Column) THEN ''tinyint''
            WHEN count(try_convert(SMALLINT, _Column)) = count(_Column) THEN ''smallint''
            WHEN count(try_convert(INT, _Column)) = count(_Column) THEN ''int''
            WHEN count(try_convert(BIGINT, _Column)) = count(_Column) THEN ''bigint''
            WHEN count(try_convert(TIME, _Column)) = count(_Column) 
                AND SUM(CASE WHEN try_convert(DATE, _Column) = ''1900-01-01'' THEN 0 ELSE 1 END) = 0
                THEN ''time''
            WHEN count(try_convert(DATETIME, _Column)) = count(_Column) THEN ''datetime''
            WHEN count(try_convert(FLOAT, _Column)) = count(_Column) THEN ''float''
            ELSE ''varchar''
        END) AS type
        FROM '+QUOTENAME(@TableName)+'
        CROSS APPLY (VALUES
            {syntax}
        ) v(ColumnName, _Column)
        WHERE _Column IS NOT NULL
        GROUP BY ColumnName;'
    EXEC sp_executesql 
    @SQLStatement,
    N'@TableName SYSNAME, {parameters}',
    @TableName=@TableName, {values};
    """

    column_names = list(dataframe.columns)
    alias_names = [str(x) for x in list(range(0,len(column_names)))]

    # develop syntax for SQL variable declaration
    declare = list(zip(
        ["DECLARE @ColumnName_"+x+" SYSNAME = ?;" for x in alias_names]
    ))
    declare = "\n".join(["\n".join(x) for x in declare])

    # develop syntax for determine data types
    syntax = list(zip(
        ["''Column"+x+"''" for x in alias_names],
        ["+QUOTENAME(@ColumnName_"+x+")+" for x in alias_names]
    ))
    syntax = ",\n".join(["\t("+x[0]+", '"+x[1]+"')" for x in syntax])

    # develop syntax for sp_executesql parameters
    parameters = ", ".join(["@ColumnName_"+x+" SYSNAME" for x in alias_names])

    # create input for sp_executesql SQL syntax
    values = ", ".join(["@ColumnName_"+x+""+"=@ColumnName_"+x+"" for x in alias_names])

    # join components into final synax
    statement = statement.format(
        declare=declare,
        syntax=syntax,
        parameters=parameters,
        values=values
    )

    # create variables for execute method
    args = [table_name] + column_names

    # execute statement
    execute(connection, statement, args)
    dtypes = connection.cursor.fetchall()
    dtypes = [x[1] for x in dtypes]
    dtypes = list(zip(column_names,dtypes))
    dtypes = {x[0]:x[1] for x in dtypes}

    # determine length of VARCHAR columns
    length = [k for k,v in dtypes.items() if v=="varchar"]
    length = subset[length].apply(lambda x: x.str.len()).max().astype('Int64')
    length = {k:"varchar("+str(v)+")" for k,v in length.items()}
    dtypes.update(length)

    return dtypes


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
    
    # search tempdb for temp tables
    if table_name.startswith("#"):
        tempdb = "tempdb."
    else:
        tempdb = ""

    table_name = safe_sql(connection, table_name)

    statement = """
    SELECT
        _columns.name AS column_name,
        TYPE_NAME(SYSTEM_TYPE_ID) AS data_type, 
        _columns.max_length, 
        _columns.precision, 
        _columns.scale, 
        _columns.is_nullable, 
        _columns.is_identity,
        _indexes.is_primary_key
    FROM {tempdb}sys.columns AS _columns
    LEFT JOIN {tempdb}sys.index_columns as _index_columns
        ON _index_columns.object_id = _columns.object_id 
        AND _index_columns.column_id = _columns.column_id
    LEFT JOIN {tempdb}sys.indexes as _indexes
        ON _indexes.object_id = _index_columns.object_id 
        AND _indexes.index_id = _index_columns.index_id
    WHERE _columns.object_ID = OBJECT_ID('{tempdb}sys.{table_name}')
    """

    statement = statement.format(tempdb=tempdb, table_name=table_name)

    # connection.cursor.execute("SELECT * FROM sys.columns WHERE sys.columns.object_ID=OBJECT_ID('"+table_name+"')").fetchall()

    schema = read_query(connection, statement)
    if len(schema)==0:
         raise errors.SQLTableDoesNotExist('{table_name} does not exist'.format(table_name=table_name)) from None
    
    schema = schema.set_index('column_name')
    schema['is_primary_key'] = schema['is_primary_key'].fillna(False)

    # define Python type equalivant of the smallest data size that is also nullable
    equal = pd.DataFrame.from_dict({
        'varchar': ['object'],
        'bit': ['boolean'],
        'tinyint': ['Int8'],
        'smallint': ['Int16'],
        'int': ['Int32'],
        'bigint': ['Int64'],
        'float': ['float64'],
        'decimal': ['float64'],
        'time': ['timedelta64[ns]'],
        'date': ['datetime64[ns]'],
        'datetime': ['datetime64[ns]'],
        'datetime2': ['datetime64[ns]']
    }, orient='index', columns=["python_type"])
    schema = schema.merge(equal, left_on='data_type', right_index=True, how='left')
    undefined = list(schema[schema['python_type'].isna()].index)
    if len(undefined)>0:
        warnings.warn("Columns : "+str(undefined), errors.DataframeUndefinedBestType)
        schema['python_type'] = schema['python_type'].fillna('str')

    return schema


def flatten_schema(schema): 
    '''Convert dataframe to a flatoutput from helpers.get_schema to inputs for table function.'''
    '''Flatten dataframe output of get_schema function into simple outputs
    
    Parameters
    ----------

    schema (pandas.DataFrame) : output from get_schema function

    Returns
    -------

    columns (dict) : keys = column names, values = data types and optionally size/precision
    not_null (list) : list of columns to set as not null, or an empty list
    primary_key_column (str, default=None) : column that is the primary key, or None
    sql_primary_key (bool) : if the primary key is an SQL identity column

    '''
    
    schema = schema.copy()

    # determine column's value
    schema[['max_length','precision','scale']] = schema[['max_length','precision','scale']].astype('str')
    schema['value'] = schema['data_type']
    # length
    dtypes = ['varchar','nvarchar']
    idx = schema['data_type'].isin(dtypes)
    schema.loc[idx, 'value'] = schema.loc[idx, 'value']+'('+schema.loc[idx,'max_length']+')'
    # precision & scale
    dtypes = ['decimal','numeric']
    idx = schema['data_type'].isin(dtypes)
    schema.loc[idx, 'value'] = schema.loc[idx, 'value']+'('+schema.loc[idx,'precision']+','+schema.loc[idx,'scale']+')'

    columns = schema['value'].to_dict()
    
    
    # non-null columns
    not_null = list(schema[~schema['is_nullable']].index)

    # primary_key_column/sql_primary_key
    primary_key_column = None
    sql_primary_key = False
    if sum(schema['is_identity'] & schema['is_primary_key'])==1:
        sql_primary_key = True
    elif sum(schema['is_primary_key'])==1:
        primary_key_column = schema[schema['is_primary_key']].index[0]

    return columns, not_null, primary_key_column, sql_primary_key


def get_pk_details(connection, table_name: str):
    ''' Get the primary key name and columns of a table.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of table to retrieve primary key name

    Returns
    -------

    primary_key_name (str) : name of the primary key
    primary_key_column (str|list) : name of the primary key column(s)

    '''

     # search tempdb for temp tables
    if table_name.startswith("#"):
        tempdb = "tempdb."
    else:
        tempdb = ""   

    table_name = safe_sql(connection, table_name)

    statement = """
    SELECT  
        _index.name AS PrimaryKeyName,
        COL_NAME(_index_columns.OBJECT_ID,_index_columns.column_id) AS PrimaryKeyColumn
    FROM
        {tempdb}sys.indexes AS _index
    INNER JOIN {tempdb}sys.index_columns AS _index_columns
        ON _index.OBJECT_ID = _index_columns.OBJECT_ID
        AND _index.index_id = _index_columns.index_id
    WHERE
        _index.is_primary_key = 1
        AND _index_columns.OBJECT_ID = OBJECT_ID('{tempdb}sys.{table_name}')
    """.format(tempdb=tempdb, table_name=table_name)

    results = read_query(connection, statement)

    if len(results)==0:
        raise errors.SQLUndefinedPrimaryKey("Table {} does not contain a primary key.".format(table_name))
    else:
        results = results.groupby(by='PrimaryKeyName')
        results = results.agg(list)
        primary_key_name = results.index[0]
        primary_key_column = results.at[primary_key_name,'PrimaryKeyColumn']
        if len(primary_key_column)==1:
            primary_key_column = primary_key_column[0]

    return primary_key_name, primary_key_column