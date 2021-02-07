# mssql_dataframe
Provides an easy & efficient interaction between Microsoft SQL and Python DataFrames. In short, once you
have data in a DataFrame, you can interact seamlessly with Microsoft SQL.

Key elements include: 
- performing tasks in SQL to avoid loading records into memory
- efficient casting between SQL and Python data types
- providing methods for SQL data engineering tasks
- follow best practices for security 


## Main Features

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