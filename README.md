# mssql_dataframe

Provides efficient mechanisms for updating and merging data into Transact-SQL tables from Python dataframes. This is accomplished by utilizing the fast_executemany feature of pyodbc to quickly insert into an SQL temporary table, and then updating/merging into a target SQL table from that temporary table. The current built-in methods for writing a dataframe to an existing SQL table allows only for the table to be replaced or records to be appended.

In practice this module may be useful for updating models, web scraping, or general data engineering tasks.

A similiar project is https://github.com/ThibTrip/pangres, but doesn't include SQL Server / Transact-SQL. The primary motivation for creating a new project is differences in Transact-SQL syntax, specifically MERGE in T-SQL vs UPSERT in other SQL flavors.

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/jwcook23/mssql_dataframe)

## Core Functionality

### Initialization

```python
from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# connect to an on-premise database using pyodbc and Windows authentication
db = connect(database_name='master', server_name='localhost')
# # or an Azure SQL database
# # db = connect(server_name='<server>.database.windows.net', username='<username>', password='<password>')

# using a single connection, initialize the class
sql = SQLServer(db)
# # or initialize the class with the ability to modify SQL objects as needed
# # sql = SQLServer(db, adjust_sql_objects=True)
```

### Update SQL Table from Python dataframe

Updating ...

```python
sql.write.update()
```

### Merge into SQL Table from Python dataframe

Merging inserts, updates, and/or deletes records depending on how records are matched between the dataframe and SQL table. This is similar to the SQL "upsert" pattern and is a wrapper around the T-SQL MERGE statement.

### Dynamic SQL Table & Column Interaction

Table and column names are passed through the stored procedure sp_executesql to prevent dynamic strings from being directly executed.

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
    

## Dependancies
[pandas](https://pandas.pydata.org/): The Python DataFrame data type.

[pyodbc](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15): The ODBC Microsoft SQL connector used.

## Quick Start

TODO: provide basic examples of core functionality


## Versioning

Version numbering MAJOR.MINOR where
- MAJOR: breaking changes
- MINOR: new features or bug fixes

## Contributing

See CONTRIBUTING.md