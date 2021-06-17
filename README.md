# mssql_dataframe
Provides an easy & efficient interaction between Microsoft SQL and Python DataFrames. In practice this module 
may be useful for model updating, data normalization, data engineering, and web scraping.

Key elements include: 
- efficiently update or merge data from Python into SQL
- automatic and dynamic SQL table and column creation/modification based on DataFrame contents
- prevention of SQL injection by utilizing the stored procedure sp_executesql, the function QUOTENAME, and the SYSNAME data type

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