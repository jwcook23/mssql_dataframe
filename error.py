import pandas as pd
import numpy as np
import pyodbc
import struct
import time

# database connection and cursor
connection = pyodbc.connect(
    driver='ODBC Driver 17 for SQL Server', server='localhost', database='master',
    autocommit=False, trusted_connection='yes'
)
cursor = connection.cursor()
cursor.fast_executemany = True

# conversion rules
## {'sql': <SQL_data_type>, 'pandas': <pandas_data_type>, 'odbc': <odbc_data_type>, 'size': <SQL_size>, 'precision': <SQL_precision>}
conversion = pd.DataFrame.from_records([
    {'sql': 'bit', 'pandas': 'boolean', 'odbc': pyodbc.SQL_BIT, 'size': 1, 'precision': 0},
    {'sql': 'tinyint', 'pandas': 'UInt8', 'odbc': pyodbc.SQL_TINYINT, 'size': 1, 'precision': 0},
    # {'sql': 'smallint', 'pandas': 'Int16', 'odbc': pyodbc.SQL_SMALLINT},
    # {'sql': 'int', 'pandas': 'Int32', 'odbc': pyodbc.SQL_INTEGER},
    # {'sql': 'bigint', 'pandas': 'Int64', 'odbc': pyodbc.SQL_BIGINT},
    # {'sql': 'float', 'pandas': 'float64', 'odbc': pyodbc.SQL_FLOAT},
    # {'sql': 'decimal', 'pandas': 'float64', 'odbc': pyodbc.SQL_DECIMAL},
    {'sql': 'time', 'pandas': 'timedelta64[ns]', 'odbc': pyodbc.SQL_SS_TIME2, 'size': 16, 'precision': 7},
    # {'sql': 'date', 'pandas': 'datetime64[ns]', 'odbc': pyodbc.SQL_TYPE_DATE},
    # {'sql': 'datetime2', 'pandas': 'datetime64[ns]', 'odbc': pyodbc.SQL_TYPE_TIMESTAMP},
    # {'sql': 'varchar', 'pandas': 'string', 'odbc': pyodbc.SQL_VARCHAR},
    # {'sql': 'nvarchar', 'pandas': 'string', 'odbc': pyodbc.SQL_WVARCHAR},
])

# sample data
## format >> {'_<SQL_data_type>': pd.Series([<SQL_lower_limit>,<SQL_upper_limit>,<SQL_null>],dtype=<pandas_data_type>)}
sample = pd.DataFrame({
    '_bit': pd.Series([False, True, None], dtype='boolean'),
    '_tinyint': pd.Series([0, 255, None], dtype='UInt8'),
    '_bigint': pd.Series([-2**63, 2**63-1, None], dtype='Int64'),
    '_time': pd.Series(['00:00:00.0000000','23:59:59.9999999',None], dtype='timedelta64[ns]')
})
## increase sample size rows
# sample = pd.concat([sample]*10000).reset_index(drop=True)
sample = pd.concat([sample]*10000).reset_index(drop=True)
## add id column for return sorting
sample.index.name = 'id'
sample = sample.reset_index()
sample['id'] = sample['id'].astype('Int64')

# insure correct testing
## insure all conversion rules are being tested
missing = ~conversion['sql'].isin([x[1::] for x in sample.columns])
if any(missing):
    missing = conversion.loc[missing,'sql'].tolist()
    raise AttributeError(f'columns missing from sample dataframe, SQL column names: {missing}')
## insure correct pandas types are being tested
check = pd.Series(sample.dtypes, name='sample')
check = check.apply(lambda x: x.name)
check.index = [x[1::] for x in check.index]
check = conversion.merge(check, left_on='sql', right_index=True)
wrong = check['pandas']!=check['sample']
if any(wrong):
    wrong = check.loc[wrong,'sql'].tolist()
    raise AttributeError(f'columns with wrong data type in sample dataframe, SQL column names: {wrong}')

# table creation
create = ',\n'.join([x+' '+x[1::].upper() for x in sample.columns if x!='id'])
create = f"""
CREATE TABLE ##conversion (
id BIGINT PRIMARY KEY,
{create}
)"""
cursor.execute(create)

# prepare write parameters
## example: cursor.setinputsizes([(<ODBC_data_type>, <SQL_size>, <SQL_precision>)])
## example: cursor.setinputsizes([(pyodbc.SQL_BIT, 1, 0), (pyodbc.SQL_SS_TIME2, 16, 7)])
query_params = pd.DataFrame(pd.Series([x.name for x in sample.dtypes], name='pandas'))
missing = ~query_params['pandas'].isin(conversion['pandas'])
if any(missing):
    missing = query_params.loc[missing,'pandas'].unique().tolist()
    raise AttributeError(f'undefined conversion for pandas data types: {missing}')    
query_params = query_params.merge(conversion)
query_params = query_params[['odbc','size','precision']].to_numpy().tolist()
query_params = [tuple(x) for x in query_params]
cursor.setinputsizes(query_params)

# prepare write values
prepped = sample.copy()
## treat NA for any pandas data type as NULL in SQL
prepped = prepped.fillna(np.nan).replace([np.nan], [None])
## SQL TIME specific formatting
prepped[['_time']] = prepped[['_time']].astype('str')
prepped[['_time']] = prepped[['_time']].replace({'None': None})
prepped[['_time']] = prepped[['_time']].apply(lambda x: x.str[7:23])

# insert data
insert = ', '.join(sample.columns)
values = ', '.join(['?']*len(sample.columns))
statement = f"""
INSERT INTO
##conversion (
    {insert}
) VALUES (
    {values}
)
"""
args = prepped.values.tolist()
cursor.executemany(statement, args)
cursor.commit()

# prepare for reading
## advantageous for types explicity defined such as pd.Timedelta
## does not apply for pandas extension types array implementations such as nullable integers that use pandas.arrays.IntegerArray
def SQL_SS_TIME2(raw_bytes):
    decode = struct.unpack("<4hI", raw_bytes)
    return pd.Timedelta((decode[0]*3600+decode[1]*60+decode[2])*1000000000+decode[4])
connection.add_output_converter(pyodbc.SQL_SS_TIME2, SQL_SS_TIME2)

# read data
time_start = time.time()
select = ', '.join([x for x in sample.columns if x!='id'])
result = cursor.execute(f'SELECT {select} FROM ##conversion ORDER BY id ASC').fetchall()
columns = [col[0] for col in cursor.description]
print('read in {} seconds'.format(time.time()-time_start))

# SQL schema for pandas data types
schema = []
for col in columns:
    x = list(list(cursor.columns(table='##conversion',catalog='tempdb',column=col).fetchone()))
    schema.append(x)
schema = pd.DataFrame(schema, columns = [x[0] for x in cursor.description])
dtypes = schema[['column_name','data_type']].merge(conversion[['odbc','pandas']], left_on='data_type', right_on='odbc')
dtypes = dtypes[['column_name','pandas']].to_numpy()
dtypes = {x[0]:x[1] for x in dtypes}

# form output using SQL schema and explicit pandas types
time_start = time.time()
result = {col: [row[idx] for row in result] for idx,col in enumerate(columns)}
result = {col: pd.Series(vals, dtype=dtypes[col]) for col,vals in result.items()}
result = pd.DataFrame(result)
print('dataframe in {} seconds'.format(time.time()-time_start))

assert result.equals(sample.drop(columns=['id']))