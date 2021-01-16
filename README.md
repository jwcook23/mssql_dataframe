# mssql_dataframe
Provides an easy & efficient interaction between Microsoft SQL and Python DataFrames. Intensive data relationship tasks are performed server side to improve overall performance. A basic example is updating records using an SQL temporary table and JOIN statement, instead of implementing the calculation in memory in Python.

Versioning numbering MAJOR.MINOR where
- MAJOR: breaking changes
- MINOR: new features or bug fixes

## Main Features

### Update Records

Update a large number of records in SQL using a DataFrame. This is a common situation in cleansing data and building models.

### Merge (Upsert) Records

Perform a merge operation. This conditionally inserts, updates, or deletes SQL records based on DataFrame contents. It is a common scenario in data engineering or webscraping.

### I/O: Server Side Computation (place holder)

Perform server side calculation and return aggregated results.

### Analytic Execution

Provides an execution tracking mechanism for analytics. This is useful for tracking the success/failure of analytics and allows a cache of data between analytic executions.

### General
1. Insert Records
2. Delete Records
3. Create and Delete Tables and Columns

## Dependancies
[pandas](https://pandas.pydata.org/): The basis of DataFrames and related calculations.

[pyodbc](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15): The perferred ODBC SQL connector.

[sqlalchemy](https://www.sqlalchemy.org/): SQL toolkit for optimizing the workflow between Python and SQL.

## Quick Start

SQL Server Developer Edition can be downloaded for free [here](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

The sample AdventureWorks database can be found [here](https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver15&tabs=ssms).

## Contributing
