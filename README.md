# mssql_dataframe
Provides an easy & efficient interaction between Microsoft SQL and Python DataFrames. Intensive tasks are performed server side if possible to improve overall performance by avoiding having to query records into local memory. Querying
records into memory is also performed efficiently by casting SQL data types to the correct DataFrame type. Security
is taken into account as best as possible as dynamic SQL tables, columns, and variable types as encountered.


## Main Features

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

[sqlalchemy](https://www.sqlalchemy.org/): SQL toolkit for optimizing the workflow between Python and SQL.

## Quick Start

SQL Server Developer Edition can be downloaded for free [here](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

The sample AdventureWorks database can be found [here](https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver15&tabs=ssms).

## Versioning

Version numbering MAJOR.MINOR where
- MAJOR: breaking changes
- MINOR: new features or bug fixes

## Contributing