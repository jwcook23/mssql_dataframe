# mssql_dataframe
# TODO: provide a basic full syntax example of sp_executesql
# TODO: provide examples for functions and validate documentation
# TODO: create a class made up of functions

Provides an easy & efficient interaction between Microsoft Transact-SQL and Python DataFrames. In practice this module 
may be useful for model updating, data normalization, data engineering, and web scraping.

## Core Functionality

### Update SQL Table from Python dataframe

Updating ...

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