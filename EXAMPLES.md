# Examples

## Initialization

Connect to an on-premise database using pyodbc. Connection to an Azure SQL database is also possible by passing a server_name in the format `server_name='<server>.database.windows.net'`along with a username and password.

If `adjust_sql_objects=True` (default is False):

1. columns will be created if they do not exist
2. column size will increase if needed, for example from TINYINT to INT
3. an SQL table will be created if it does not exist

With `adjust_sql_objects=True` exceptions will still be raised in certian cases to preserve integrity:

1. prevent changing SQL data type from number like to string
2. prevent changing column nullability

```python
import time
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# # connect to database using pyodbc
db = connect(database_name='master', server_name='localhost')
# # initialize the main package
sql = SQLServer(db, adjust_sql_objects=True)
```

## Creating SQL Tables

SQL tables can be simply created from a dataframe object.

Note:

1. a "best fit" SQL data type is determined in SQL
2. `primary_key='index'` creates an SQL primary key based on the dataframe's index

Here a global temporary is created, but in practice a user table would be created so it persists in the database after the database connection is closed.

``` python
# create a large sample dataframe of 100,000 records
df = pd.DataFrame({
    'ColumnA': list(range(0,100000,1)),
    'ColumnB': [0]*100000 
})
df.index.name = 'PK_Column'
# create the table with the index as the SQL primary key
df = sql.create.table_from_dataframe(table_name='##sample_update', dataframe=df,
    primary_key='index'
)
```

## Insert from Dataframe

100,000 records are inserted in approximately 2 seconds in a localhost database.

Note:

1. since `include_timestamps=True` a new column named _time_insert is created automatically.

``` python
time_start = time.time()
sql.write.insert(table_name='##sample_update', dataframe=df, include_timestamps=True)
print('Inserted {} records in {} seconds'.format(len(df), round(time.time()-time_start,2)))
```

## Reading into Dataframe

Reading data from an SQL table into a dataframe is straight-forward and allows for a limit, order, and where conditions.

Note:

1. SQL primary key column "PK_Column" has been placed as the dataframe's index.

``` python
result = sql.read.select('##sample_update', limit=5)
result
```

## Update from Dataframe

100,000 records are updated in approximately 3 seconds in a localhost database.

Note:

1. a new _time_update column is created since `include_timestamps=True`
2. ColumnC is created with data type VARCHAR(4) since that is ColumnC's max length
3. ColumnB is changed from data type TINYINT to INT to fit the updated values
4. since `match_columns=None`, the SQL primary key / dataframe index is used to perform the update

``` python
# update a dataframe column
df['ColumnB'] = list(range(0,100000,1))
# create a new dataframe column
df['ColumnC'] = 'aaaa'
time_start = time.time()
# update records in the target SQL table
sql.write.update('##sample_update', df[['ColumnB','ColumnC']], match_columns=None)
print('Updated {} records in {} seconds'.format(len(df), round(time.time()-time_start,2)))
```

Any size dataframe can be used to update matching records in SQL. Here match_columns is specified instead of using the dataframe's index/SQL primary key.

``` python
# update ColumnA to -1 where ColumnB=0
df_small = pd.DataFrame({'ColumnB': [0], 'ColumnA': [-1]})
time_start = time.time()
# update the target table using columns
sql.write.update('##sample_update', df_small[['ColumnB','ColumnA']],
    match_columns = ['ColumnB']
)
print('Updated ? records in {} seconds'.format(round(time.time()-time_start,2)))
```

## Merge from Dataframe

Merging inserts, updates, and/or deletes records in a target SQL table depending on how records are matched. This uses the T-SQL MERGE statement. This also can cover an UPSERT type action (update if exists, otherwise insert).

First, create a sample table to merge into.

``` python
df_merge = pd.DataFrame({
    'State': ['A','B'],
    'ColumnA': [1,2],
    'ColumnB': ['a','b']
}, index=[0,1])
df_merge.index.name = 'PK_Column'
df_merge = sql.create.table_from_dataframe(table_name='##sample_merge', dataframe=df_merge,
    primary_key='index'
)
sql.write.insert(table_name='##sample_merge', dataframe=df_merge, include_timestamps=True)
result = sql.read.select('##sample_merge', limit=5)
result
```

Similate dataframe records that have been deleted/updated/added before merging them into the target table.

```python
df_source = df_merge.copy()
# a deleted record
df_source = df_source[df_source.index!=0]
# an updated record
df_source.loc[1,'ColumnA'] = 3
# a new record
df_append = pd.DataFrame({'State': ['C'], 'ColumnA': [6], 'ColumnB': ['d']}, index=[2])
df_append.index.name = 'PK_Column'
df_source = df_source.append(df_append)
```

Performing the merge, note:

1. a new _time_update column since `include_timestamps=True`
2. the record where State=A has been deleted
3. the record where State=B has been updated
4. a new record has been inserted for State=C

``` python
sql.write.merge(table_name='##sample_merge', dataframe=df_source, 
    match_columns=['PK_Column','State'], include_timestamps=True
)
result = sql.read.select('##sample_merge', limit=5)
result
```

It's possible to specify additional critera for record deletion. This is useful in case records are incrementally being merged from a dataframe. Use cases for incremental loading may be long running web scraping or model training tasks.

Note:

1. a match on State is also required for a record to be deleted
2. the record where State=A remains in the table since the delete condition was not met
3. one of the records for State=B is updated (since _pk=1 is in the dataframe)
4. one of the records for State=B is deleted (since _pk=2 is not in the dataframe)
5. a record for State=C has been inserted

``` python
# create a sample table and insert sample records
df_condition = pd.DataFrame({
    'State': ['A','B','B'],
    'ColumnA': [3,4,4],
    'ColumnB': ['a','b','b']
}, index=[0,1,2])
df_condition.index.name='_pk'
df_condition = sql.create.table_from_dataframe("##sample_merge_delete_condition", df_condition, 
    primary_key='index'
)
sql.write.insert("##sample_merge_delete_condition", df_condition, include_timestamps=True)

# simulate deleted records
df_condition = df_condition[df_condition.index==1]
# simulate updated records
df_condition.loc[df_condition.index==1,'ColumnA'] = 5
df_condition.loc[df_condition.index==1,'ColumnB'] = 'c'
# simulate new record
df_condition = df_condition.append(
    pd.DataFrame({'State':['C'],'ColumnA':[6],'ColumnB':['d']},index=[3])
)
df_condition.index.name = '_pk'

# perform merge
sql.write.merge('##sample_merge_delete_condition', df_condition, match_columns=['_pk'], 
    delete_conditions=['State']
)
result = sql.read.select('##sample_merge_delete_condition', limit=5)
result
```

Performing an upsert action (if exists update, otherwise insert) is possible by passing in the parameter `delete_unmatched=False`.

Note:

1. the record where State=A remains after the merge

``` python
# create a sample table and insert sample records
df_upsert = pd.DataFrame({
    'ColumnA': [3,4]
})
df_upsert = sql.create.table_from_dataframe("##sample_upsert", df_upsert, primary_key='index')
sql.write.insert("##sample_upsert", df_upsert, include_timestamps=False)

# simulate a deleted record
df_upsert = df_upsert[df_upsert.index!=0]
# simulate an updated record
df_upsert.loc[df_upsert.index==1,'ColumnA'] = 5
# simulate a new record
df_upsert = df_upsert.append(pd.Series([6], index=['ColumnA'], name=2))

# perform the merge
sql.write.merge('##sample_upsert', df_upsert, delete_unmatched=False)
result = sql.read.select('##sample_merge_delete_condition', limit=5)
result
```

## Altering SQL Objects Manually

If this module creates/alters an SQL object in an undesired manner, the underlying modify methods can be manually used to correct them.

Note:

1. Column1 was initially inferred to be a BIT/BOOLEAN as it consisted of only 1s and 0s.

```python
# import function to get SQL schema
from mssql_dataframe.core.helpers import get_schema
# create a sample SQL table
df_modify = pd.DataFrame({'Column1': [0,1,0,1]})
df_modify = sql.create.table_from_dataframe('##sample_modify', df_modify)
sql.write.insert('##sample_modify', df_modify)
# get SQL schema and records
schema = get_schema(sql.connection, '##sample_modify')
result = sql.read.select('##sample_modify')
schema[['data_type','is_nullable']]
result
# manually change the SQL data type
sql.modify.column('##sample_modify', 'alter', 'Column1', data_type='TINYINT', 
    not_null=False
)
schema = get_schema(sql.connection, '##sample_modify')
result = sql.read.select('##sample_modify')
schema[['data_type','is_nullable']]
result
```

## Dynamic SQL

Table and column names are passed through the stored procedure sp_executesql and/or use the T-SQL function QUOTENAME to prevent any dynamic strings from being directly executed.

For example, a column is added to a table using the pattern:

```python
statement = '''
DECLARE @SQLStatement AS NVARCHAR(MAX);
DECLARE @TableName SYSNAME = ?;
DECLARE @ColumnName SYSNAME = ?;
DECLARE @ColumnType SYSNAME = ?;

SET @SQLStatement = 
    N'ALTER TABLE '+QUOTENAME(@TableName)+
    'ADD' +QUOTENAME(@ColumnName)+' '+QUOTENAME(@ColumnType)';'

EXEC sp_executesql 
    @SQLStatement,
    N'@TableName SYSNAME, @ColumnName SYSNAME, @ColumnType SYSNAME',
    @TableName=@TableName, @ColumnName=@ColumnName, @ColumnType=@ColumnType;
'''

args = ['DynamicSQLTableName','DynamicSQLColumnName','DynamicSQLDataType']
cursor.execute(statment, *args)
```
