# mssql_dataframe
# TODO: provide a basic full syntax example of sp_executesql
# TODO: provide examples for functions and validate documentation
# TODO: clean up imports
# TODO: create a class made up of functions
# TODO: validate workflow of primary keys during update & merge
# TODO: default update & merge on columsn to primary key (allow non-primary key?)
Provides an easy & efficient interaction between Microsoft Transact-SQL and Python DataFrames. In practice this module 
may be useful for model updating, data normalization, data engineering, and web scraping.

## Core Functionality

### Update SQL Table from Python dataframe

Updating ...

### Merge into SQL Table from Python dataframe

Merging inserts, updates, and/or deletes records depending on how records are matched between the dataframe and SQL table. This is similar to the SQL "upsert" pattern and is a wrapper around the T-SQL MERGE statement.

### Dynamic SQL Table & Column

DECLARE @SQLStatement AS NVARCHAR(MAX);
DECLARE @TableName SYSNAME = ?;
DECLARE @ColumnName SYSNAME = ?;
DECLARE @ColumnType SYSNAME = ?;
DECLARE @ColumnSize SYSNAME = ?;

SET @SQLStatement = 
    N'ALTER TABLE '+QUOTENAME(@TableName)+
    'ADD' +QUOTENAME(@ColumnName)+' '+QUOTENAME(@ColumnType)+' '+@ColumnSize+';'

EXEC sp_executesql 
    @SQLStatement,
    N'@TableName SYSNAME, @ColumnName SYSNAME, @ColumnType SYSNAME, @ColumnSize VARCHAR(MAX)',
    @TableName=@TableName, @ColumnName=@ColumnName, @ColumnType=@ColumnType, @ColumnSize=@ColumnSize;
    

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