# mssql_dataframe

Provides efficient mechanisms for updating and merging data into Transact-SQL tables from Python dataframes. This is accomplished by utilizing the fast_executemany feature of pyodbc to quickly insert into an SQL temporary table, and then updating/merging into a target SQL table from that temporary table.

In practice this module may be useful for updating models, web scraping, or general data engineering tasks.

A similiar project is https://github.com/ThibTrip/pangres, but doesn't include SQL Server / Transact-SQL. The primary motivation for creating a new project is differences in Transact-SQL syntax, specifically MERGE in T-SQL vs UPSERT in other SQL flavors.

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/jwcook23/mssql_dataframe)

## Dependancies
[pandas](https://pandas.pydata.org/): The Python DataFrame.

[pyodbc](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15): The ODBC driver used for Transact-SQL statements.

## Core Functionality

### Initialization

Connect to an on-premise database using pyodbc. Connection to an Azure SQL database is also possible by passing a server_name like `server_name='<server>.database.windows.net'`along with a username and password.

By default `adjust_sql_objects==False`, but if True, mssql_dataframe has the ability to add columns or change column data types if needed.

```python
import time
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# # connect using pyodbc
db = connect(database_name='master', server_name='localhost')
# # initialize the main package
sql = SQLServer(db, adjust_sql_objects=True)
```

### Creating SQL Tables

SQL tables can be simply created from a dataframe object. A "best fit" SQL data type is automatically determined in SQL.

Here a global temporary is created, but in practice a user table would be created so it persists in the database after the connection is closed.

``` python
df = pd.DataFrame({
    'ColumnA': list(range(0,100000,1)),
    'ColumnB': [0]*100000 
})
df.index.name = 'PK_Column'
# # create the table, setting the dataframe index as the SQL primary key
df = sql.create.table_from_dataframe(table_name='##update_mssql_dataframe', dataframe=df, primary_key='index')
```

### Insert from Dataframe

100,000 records are inserted in approximately 2 seconds.

Note that since `include_timestamps=True` a new column is created.

``` python
time_start = time.time()
sql.write.insert(table_name='##update_mssql_dataframe', dataframe=df, include_timestamps=True)
print('Inserted {} records in {} seconds'.format(len(df), round(time.time()-time_start,2)))

```

### Reading into Dataframe

Reading data from an SQL table into a dataframe is straightforward.

Notes:
1. SQL primary key column "PK_Column" has been placed as the dataframe's index.

``` python
result = sql.read.select('##update_mssql_dataframe', limit=5)
result
```

#### Update from Dataframe

100,000 records are updated in approximately 3 seconds. Since `match_columns=None`, the SQL primary key / index of the dataframe is used to update values.

Notes:
1. a new _time_update column since `include_timestamps=True`
2. Column C is created with data type VARCHAR(4)
3. ColumnB is changed from data type TINYINT to INT

``` python
dataframe['ColumnB'] = list(range(0,100000,1))
dataframe['ColumnC'] = 'aaaa'
time_start = time.time()
sql.write.update(table_name, dataframe[['ColumnB','ColumnC']], match_columns=None)
print('Updated {} records in {} seconds'.format(len(dataframe), round(time.time()-time_start,2)))
```

Any size dataframe can be used to update the records in SQL. Here match_columns is specified.

``` python
dataframe = pd.DataFrame({'ColumnB': [0], 'ColumnA': [-1]})
time_start = time.time()
sql.write.update(table_name, dataframe[['ColumnB','ColumnA']], match_columns = ['ColumnB'])
print('Updated ? records in {} seconds'.format(round(time.time()-time_start,2)))
```

### Merge from Dataframe

Merging inserts, updates, and/or deletes records depending on how records are matched between the dataframe and SQL table. This uses the T-SQL MERGE statement.

Create a sample table to merge into.

``` python
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
```

Similate records that have been deleted/updated/added.

```python
df_new = df_merge.copy()
# a deleted record
df_new = df_new[df_new.index!=0]
# an updated record
df_new.loc[1,'ColumnA'] = 3
# a new record
df_append = pd.DataFrame({'State': ['C'], 'ColumnA': [6], 'ColumnB': ['d']}, index=[2])
df_append.index.name = 'PK_Column'
df_new = df_new.append(df_append)
```

Performing the merge, note that:

1. a new _time_update column since `include_timestamps=True`
2. the record where State=A has been deleted
3. the record where State=B has been updated
3. a new record has been inserted for State=C

``` python
sql.write.merge(table_name='##merge_mssql_dataframe', dataframe=df_new, match_columns=['PK_Column','State'], include_timestamps=True)
result = sql.read.select('##merge_mssql_dataframe', limit=5)
result
```

TODO: showcase subset_columns
TODO: introduce option to not delete records

### Dynamic SQL Table & Column Interaction

Table and column names are passed through the stored procedure sp_executesql to prevent any dynamic strings from being directly executed.

For example, a column is added to a table using:

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

## Versioning

Version numbering MAJOR.MINOR where
- MAJOR: breaking changes
- MINOR: new features or bug fixes

## Contributing

See CONTRIBUTING.md