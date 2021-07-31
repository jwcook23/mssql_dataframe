import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core.helpers import get_schema

db = connect(database_name='master', server_name='localhost')
# # initialize the main package
sql = SQLServer(db, adjust_sql_objects=True)

dataframe = pd.read_csv(r'C:\Users\jacoo\Desktop\Temp\test.csv')
dataframe = dataframe[dataframe.columns[1::]]
# BUG: unable to use .astype('str',skipna=True) https://github.com/pandas-dev/pandas/issues/25353
dataframe = dataframe.replace({'': None, 'None': None, 'nan': None, 'NaT': None, '<NA>': None})

table_name = '##test_error'

columns = dataframe.columns[[2,31]]
# size = int(dataframe[columns].str.len().max())
# columns = {columns: 'varchar('+str(size)+')'}
# sql.create.table(table_name, columns)

# sql.write.insert(table_name, dataframe[list(columns.keys())], include_timestamps=False)

sql.write.merge(table_name, dataframe)


# TODO: make created table's schema clearly evident
# TODO: pass through integrity errors instead of hiding (duplicate primary key, null value in non-null)
# TODO: composite primary key


# create table
# add _time_insert
# column not created with adequate size?
# attempted fix, same column size found
# integrity error (duplicate primary key)

# ProgrammingError('String data, right truncation: length 94 buffer 92', 'HY000')

# TODO: dataframe.at[18780,'Practice-Location-Address-Line1'] = ' None'

schema = get_schema(db, table_name)

sql.connection.cursor.execute(f'SELECT * FROM {table_name}').fetchall()