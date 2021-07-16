
# Intialization

import time
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# # connect using pyodbc
db = connect(database_name='master', server_name='localhost')
# # initialize the main package
sql = SQLServer(db, adjust_sql_objects=True)


# Creating SQL Tables
df = pd.DataFrame({
    'ColumnA': list(range(0,100000,1)),
    'ColumnB': [0]*100000 
})
df.index.name = 'PK_Column'
# # create the table, setting the dataframe index as the SQL primary key
df = sql.create.table_from_dataframe(table_name='##update_mssql_dataframe', dataframe=df, primary_key='index')


# Insert
time_start = time.time()
sql.write.insert(table_name='##update_mssql_dataframe', dataframe=df, include_timestamps=True)
print('Inserted {} records in {} seconds'.format(len(df), round(time.time()-time_start,2)))

# Read
result = sql.read.select('##update_mssql_dataframe', limit=5)
result

# Update
df['ColumnB'] = list(range(0,100000,1))
df['ColumnC'] = 'aaaa'
time_start = time.time()
sql.write.update(table_name='##update_mssql_dataframe', dataframe=df[['ColumnB','ColumnC']], match_columns=None)
print('Updated {} records in {} seconds'.format(len(df), round(time.time()-time_start,2)))
result = sql.read.select('##update_mssql_dataframe', limit=5)
result

# Merge
df_merge = pd.DataFrame({
    'State': ['A','B'],
    'ColumnA': [1,2],
    'ColumnB': ['a','b']
}, index=[0,1])
df_merge.index.name = 'PK_Column'
dataframe = sql.create.table_from_dataframe(table_name='##merge_mssql_dataframe', dataframe=df_merge, primary_key='index')
sql.write.insert(table_name='##merge_mssql_dataframe', dataframe=df_merge, include_timestamps=True)
result = sql.read.select('##merge_mssql_dataframe', limit=5)
result

df_new = df_merge.copy()
# # simulate a deleted record
df_new = df_new[df_new.index!=0]
# # simulate an updated record
df_new.loc[1,'ColumnA'] = 3
# # simulate a new record
df_append = pd.DataFrame({'State': ['C'], 'ColumnA': [6], 'ColumnB': ['d']}, index=[2])
df_append.index.name = 'PK_Column'
df_new = df_new.append(df_append)
# # merge the dataframe in the SQL table
sql.write.merge(table_name='##merge_mssql_dataframe', dataframe=df_new, match_columns=['PK_Column','State'], include_timestamps=True)
result = sql.read.select('##merge_mssql_dataframe', limit=5)
result









import time
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# Initialization
# # connect to an on-premise database using pyodbc (connection to Azure SQL database also possible)
db = connect(database_name='master', server_name='localhost')
# # initialize with the ability to create and alter objects (by default, adjust_sql_objects==False)
sql = SQLServer(db, adjust_sql_objects=True)
 
# Updating an SQL Table
# # create a sample table to update
# # note, records are inserted along with a new _time_insert column
table_name = '##sample_update'
dataframe = pd.DataFrame({
    'ColumnA': list(range(0,100000,1)),
    'ColumnB': [0]*100000 
})
dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
time_start = time.time()
sql.write.insert(table_name, dataframe)
print('Inserted {} records in {} seconds'.format(len(dataframe), round(time.time()-time_start,2)))

# # update SQL table using dataframe's index & the SQL primary key
# # note, records are updated along with a new _time_update column
# # note, ColumnC is created with the data type VARCHAR(4) in SQL
# # note, the data type of ColumnB is changed from data type TINYINT to INT in SQL
dataframe['ColumnB'] = list(range(0,100000,1))
dataframe['ColumnC'] = 'aaaa'
time_start = time.time()
sql.write.update(table_name, dataframe[['ColumnB','ColumnC']])
print('Updated {} records in {} seconds'.format(len(dataframe), round(time.time()-time_start,2)))

# # update SQl table using specific column(s)
# # note, the dataframe used for updating the SQL table can be of any size
dataframe = pd.DataFrame({'ColumnB': [0], 'ColumnA': [-1]})
time_start = time.time()
sql.write.update(table_name, dataframe[['ColumnB','ColumnA']], match_columns = ['ColumnB'])
print('Updated ? records in {} seconds'.format(round(time.time()-time_start,2)))

print('x')