# mssql_dataframe
Provides an easy & efficient interaction between Microsoft SQL and Python DataFrames. In essence, this module 
may be useful for model updating, data normalization, data engineering, and web scraping.

Key elements include: 
- write and read seamlessly between SQL tables and DataFrames
- advanced methods for updating and merge DataFrames into SQL tables
- dynamic SQL object creation including tables and columns with SQL data type determination
-TODO: calculations server-side to limit I/O and avoid having to load records into memory


## Main Features

TODO: provide concrete basic examples of core functionality

### Create SQL Table

Create a table in SQL in a manner to prevent SQL injection given variable table and column names. SQL data types, including variable size are automatically inferred.

### Update Records

Update a large number of records in SQL using a DataFrame. This is a common situation in cleansing data and building models.

### Merge (Upsert) Records

Perform a merge operation. This conditionally inserts, updates, or deletes SQL records based on DataFrame contents. It is a common scenario in data engineering or webscraping.

### I/O: Server Side Computation (place holder)

Perform server side calculation and return aggregated results.

### Analytic Execution

Provides an execution tracking mechanism for analytics. This is useful for tracking the success/failure of analytics and allows a cache of data between analytic executions.

### SQL Injection Prevention

Dynamic SQL, the QUOTENAME function, and the SYSNAME datatype are used to prevent SQL injection.
This example demostrates what occurs under-the-hood for creating a table with a single column "x". A simliar
approach is used throughout this module for variable table and column names.

```python
statement = """
DECLARE @SQLStatement AS NVARCHAR(MAX);
DECLARE @TableName SYSNAME = ?;
DECLARE @ColumnName_x SYSNAME = ?;
DECLARE @ColumnType_x SYSNAME = ?;
DECLARE @ColumnSize_x SYSNAME = ?;

SET @SQLStatement = N'CREATE TABLE '+QUOTENAME(@TableName)+' ('+
QUOTENAME(@ColumnName_x)+' '+QUOTENAME(@ColumnType_x)+' '+@ColumnSize_x+
');'

EXEC sp_executesql @SQLStatement,
N'@TableName SYSNAME, @ColumnName_x SYSNAME, @ColumnType_x SYSNAME, @ColumnSize_x VARCHAR(MAX)',
@TableName=@TableName, @ColumnName_x=@ColumnName_x, @ColumnType_x=@ColumnType_x, @ColumnSize_x=@ColumnSize_x;
"""

args = ['NameOfTable','NameOfColumn','VARCHAR(100)']

cursor.execute(statement, *args)
```

In practice, this complicated statement is built simply using the create_table function of this module.

```python
create_table(table='TableName', columns={'ColumnName': 'VARCHAR(100)'})
```

## Dependancies
[pandas](https://pandas.pydata.org/): The basis of DataFrames and related calculations.

[pyodbc](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15): The perferred ODBC SQL connector.


## Quick Start

SQL Server Developer Edition can be downloaded for free [here](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

The sample AdventureWorks database can be found [here](https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver15&tabs=ssms).

## Versioning

Version numbering MAJOR.MINOR where
- MAJOR: breaking changes
- MINOR: new features or bug fixes

## Contributing

Add appropriate pytest functions to mssql_dataframe/tests.

Run tests for mssql_dataframe only.

```cmd
pytest --cov tests/
```

If you encounter issus when running tests, try to collect tests only.

```cmd
pytest --collect-only
```