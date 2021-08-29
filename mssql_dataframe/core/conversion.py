''' Handles base funcationality of data movement between Python pandas and SQL. Includes conversion rules between SQL/ODBC/pandas.
'''
import warnings
import struct

import pyodbc
import pandas as pd
import numpy as np

from mssql_dataframe.core import errors

rules = pd.DataFrame.from_records([
    {'sql': 'bit', 'pandas': 'boolean', 'odbc': pyodbc.SQL_BIT, 'size': 1, 'precision': 0},
    {'sql': 'tinyint', 'pandas': 'UInt8', 'odbc': pyodbc.SQL_TINYINT, 'size': 1, 'precision': 0},
    {'sql': 'smallint', 'pandas': 'Int16', 'odbc': pyodbc.SQL_SMALLINT, 'size': 2, 'precision': 0},
    {'sql': 'int', 'pandas': 'Int32', 'odbc': pyodbc.SQL_INTEGER, 'size': 4, 'precision': 0},
    {'sql': 'bigint', 'pandas': 'Int64', 'odbc': pyodbc.SQL_BIGINT, 'size': 8, 'precision': 0},
    {'sql': 'float', 'pandas': 'float64', 'odbc': pyodbc.SQL_FLOAT, 'size': 8, 'precision': 53},
    {'sql': 'time', 'pandas': 'timedelta64[ns]', 'odbc': pyodbc.SQL_SS_TIME2, 'size': 16, 'precision': 7},
    {'sql': 'date', 'pandas': 'datetime64[ns]', 'odbc': pyodbc.SQL_TYPE_DATE, 'size': 10, 'precision': 0},
    {'sql': 'datetime2', 'pandas': 'datetime64[ns]', 'odbc': pyodbc.SQL_TYPE_TIMESTAMP, 'size': 27, 'precision': 7},
    {'sql': 'varchar', 'pandas': 'string', 'odbc': pyodbc.SQL_VARCHAR, 'size': 0, 'precision': 0},
    {'sql': 'nvarchar', 'pandas': 'string', 'odbc': pyodbc.SQL_WVARCHAR, 'size': 0, 'precision': 0},
])
rules['sql'] = rules['sql'].astype('string')


def get_schema(cursor, table_name, columns):
    '''Get schema of an SQL table, as well as the defined conversion rules.

    Parameters
    ----------
    cursor (pyodbc.Cursor) : cursor for executing statement
    table_name (str) : table name containing columns
    columns (list) : column names of schema to get

    Returns
    -------
    '''

    # insure temp tables are found
    if table_name.startswith('##'):
        catalog = 'tempdb'
    else:
        catalog = None

    # get schema
    schema = []
    for col in columns:
        x = list(cursor.columns(table=table_name,catalog=catalog,column=col).fetchone())
        schema.append(x)
    schema = pd.DataFrame(schema, columns = [x[0] for x in cursor.description])
    schema = schema[['column_name','data_type','type_name','is_nullable','ss_is_identity']]
    schema[['column_name','type_name']] = schema[['column_name','type_name']].astype('string')
    schema['is_nullable'] = schema['is_nullable']=='YES'
    schema['ss_is_identity'] = schema['ss_is_identity']==1

    # add primary key info
    pk = cursor.primaryKeys(table=table_name, catalog=catalog).fetchall()
    pk = pd.DataFrame([list(x) for x in pk], columns=[x[0] for x in cursor.description])
    pk = pk.rename(columns={'key_seq': 'pk_seq'})
    schema = schema.merge(pk[['column_name','pk_seq','pk_name']], left_on='column_name', right_on='column_name', how='left')
    schema['pk_seq'] = schema['pk_seq'].astype('Int64')
    schema['pk_name'] = schema['pk_name'].astype('string')

    # add conversion rules
    schema = schema.merge(rules[['pandas','odbc','size','precision']], left_on='data_type', right_on='odbc', how='left')
    schema = schema.drop(columns=['data_type'])

    # key column_name as index, check for undefined conversion rule
    schema = schema.set_index(keys='column_name')
    missing = schema[['pandas','odbc','size','precision']].isna().any(axis='columns')
    if any(missing):
        missing = missing[missing].index.tolist()
        raise AttributeError(f'undefined conversion for columns: {missing}')  

    return schema


def prepare_cursor(schema, dataframe, cursor):
    '''
    Prepare cursor for writting values to SQL.

    Parameters
    ----------
    schema (pandas.DataFrame) : output from get_schema function
    dataframe (pandas.DataFrame) : values to be written to SQL, used to determine size of string columns
    cursor (pyodbc.Cursor) : cursor to be used to write values

    Returns
    -------
    cursor (pyodbc.Cursor) : cursor with SQL data type and size parameters set
    '''

    schema = schema[['odbc','size','precision']]
    columns = pd.Series(dataframe.columns, name='column_name')
    missing = ~columns.isin(schema.index)
    if any(missing):
        missing = columns[~missing].to_list()
        raise errors.SQLColumnDoesNotExist(f'SQL columns do not exist: {missing}')

    # insure columns are sorted correctly 
    schema = schema.loc[dataframe.columns]
    
    # use dataframe contents to determine size for strings
    infer = schema[schema['size']==0].index
    infer = dataframe[infer].apply(lambda x: x.str.len()).max()
    infer = pd.DataFrame(infer, columns=['size'])
    schema.update(infer)
    schema['size'] = schema['size'].astype('int64')

    # set SQL data type and size for cursor
    schema = schema[['odbc','size','precision']].to_numpy().tolist()
    schema = [tuple(x) for x in schema]
    cursor.setinputsizes(schema)

    return cursor


def prepare_values(schema, dataframe):
    '''Prepare dataframe contents for writting values to SQL.
    
    Parameters
    ----------

    dataframe (pandas.DataFrame) : values to be written to SQL

    Returns
    -------

    dataframe (pandas.DataFrame) : values that may have been truncated due to SQL precision limitations
    values (list) : values to pass to pyodbc cursor execuatemany

    '''

    prepped = dataframe.copy()

    # SQL data type TIME as string since python datetime.time allows 6 decimal places but SQL allows 7
    dtype = schema[schema['odbc']==pyodbc.SQL_SS_TIME2].index
    truncation = prepped[dtype].apply(lambda x: any(x.dt.nanoseconds%100>0))
    if any(truncation):
        truncation = list(truncation[truncation].index)
        warnings.warn(f'Nanosecond precision for columns {truncation} will be truncated as TIME allows 7 max decimal places..')
        nanosecond = dataframe[dtype].apply(lambda x: pd.to_timedelta((x.dt.nanoseconds//100)*100))
        dataframe[dtype] = dataframe[dtype].apply(lambda x: x.dt.floor(freq='us'))
        dataframe[dtype] = dataframe[dtype]+nanosecond
    invalid = ((prepped[dtype]>=pd.Timedelta(days=1)) | (prepped[dtype]<pd.Timedelta(days=0))).any()
    if any(invalid):
        invalid = list(invalid[invalid].index)
        raise ValueError(f'columns {invalid} are out of range for SQL TIME data type. Allowable range is 00:00:00.0000000-23:59:59.9999999')
    prepped[dtype] = prepped[dtype].astype('str')
    prepped[dtype] = prepped[dtype].replace({'NaT': None})
    prepped[dtype] = prepped[dtype].apply(lambda x: x.str[7:23])

    # SQL data type DATETIME2 as string since python datetime.datetime allows 6 decimals but SQL allows 7
    dtype = schema[schema['odbc']==pyodbc.SQL_TYPE_TIMESTAMP].index
    truncation = prepped[dtype].apply(lambda x: any(x.dt.nanosecond%100>0))
    if any(truncation):
        truncation = list(truncation[truncation].index)
        warnings.warn(f'Nanosecond precision for columns {truncation} will be truncated as DATETIME2 allows 7 max decimal places.')
        nanosecond = dataframe[dtype].apply(lambda x: pd.to_timedelta((x.dt.nanosecond//100)*100))
        dataframe[dtype] = dataframe[dtype].apply(lambda x: x.dt.floor(freq='us'))
        dataframe[dtype] = dataframe[dtype]+nanosecond

    prepped[dtype] = prepped[dtype].astype('str')
    prepped[dtype] = prepped[dtype].replace({'NaT': None})
    prepped[dtype] = prepped[dtype].apply(lambda x: x.str[0:27])

    # treat pandas NA,NaT,etc as NULL in SQL
    prepped = prepped.fillna(np.nan).replace([np.nan], [None])

    # values to pyodbc cursor executemany
    values = prepped.values.tolist()

    return dataframe, values


def prepare_connection(connection):
    ''' Prepare connection by adding output converters.
    
    Parameters
    ----------
    connection (pyodbc.Connection) : connection without default output converters

    Returns
    -------
    connection (pyodbc.Connection) : connection with added output converters

    Advantages:

    1. conversion to base Python type isn't defined by the ODBC library which will raise an error such as:
    - pyodbc.ProgrammingError: ('ODBC SQL type -155 is not yet supported. column-index=0 type=-155', 'HY106')
    
    2. conversion directly to a pandas type allows greater precision such as:
    - python datetime.datetime allows 6 decimal places of precision while pandas Timestamps allows 9

    Sidenotes:
    1. adding converters for nullable pandas integer types isn't possible, since those are implemented at the array level
    2. pandas doesn't have an exact precision decimal data type

    '''

    # TIME (pyodbc.SQL_SS_TIME2, SQL TIME)
    ## python datetime.time has 6 decimal places of precision and isn't nullable
    ## pandas Timedelta supports 9 decimal places and is nullable
    ## SQL TIME only supports 7 decimal places for precision
    ## SQL TIME range is '00:00:00.0000000' to '23:59:59.9999999' while pandas allows multiple days and negatives
    def SQL_SS_TIME2(raw_bytes, pattern=struct.Struct("<4hI")):
        hour, minute, second, _, fraction = pattern.unpack(raw_bytes)
        return pd.Timedelta(hours=hour, minutes=minute, seconds=second, microseconds=fraction//1000, nanoseconds=fraction%1000)
    connection.add_output_converter(pyodbc.SQL_SS_TIME2, SQL_SS_TIME2)

    # TIMESTAMP (pyodbc.SQL_TYPE_TIMESTMAP, SQL DATETIME2)
    ## python datetime.datetime has 6 decimal places of precision and isn't nullable
    ## pandas Timestamp supports 9 decimal places and is nullable
    ## SQL DATETIME2 only supports 7 decimal places for precision
    ## pandas Timestamp range range is '1677-09-21 00:12:43.145225' to '2262-04-11 23:47:16.854775807' while SQL allows '0001-01-01' through '9999-12-31'
    def SQL_TYPE_TIMESTAMP(raw_bytes, pattern=struct.Struct("hHHHHHI")):
        year, month, day, hour, minute, second, fraction = pattern.unpack(raw_bytes)
        return pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute, second=second, microsecond=fraction//1000, nanosecond=fraction%1000)
    connection.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP)

    return connection


def insert_values(table_name, columns, values, cursor):
    ''' Insert values into a table.

    Parameters
    ----------
    table_name (str) : table to insert into
    columns (list) : columns to insert into
    values (list) : values to insert
    cursor (pyodbc.Cursor) : database cursor to perform transaction

    Returns
    -------
    None
    '''

    insert = ', '.join(columns)
    params = ', '.join(['?']*len(columns))
    statement = f"""
    INSERT INTO
    {table_name} (
        {insert}
    ) VALUES (
        {params}
    )
    """
    cursor.executemany(statement, values)
    cursor.commit()


def read_values(statement, schema, cursor):
    ''' Read data from SQL into a pandas dataframe.

    Parameters
    ----------
    statement (str) : statement to execute to get data
    schema (pandas.DataFrame) : output from get_schema function for setting dataframe data types
    cursor (pyodbc.Cursor) : database cursor for reading data

    Returns
    -------
    result (pandas.DataFrame) : resulting data from performing statement

    '''

    # read data from SQL
    result = cursor.execute(statement).fetchall()
    columns = [col[0] for col in cursor.description]

    # form output using SQL schema and explicit pandas types
    dtypes = schema.loc[columns,'pandas'].to_dict()
    result = {col: [row[idx] for row in result] for idx,col in enumerate(columns)}
    result = {col: pd.Series(vals, dtype=dtypes[col]) for col,vals in result.items()}
    result = pd.DataFrame(result)

    return result

