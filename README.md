<!-- TODO:  document cursor commit where needed-->
<!-- TODO: type hints, including returns -->

# mssql_dataframe

![Tests Status](./reports/tests.svg?dummy=8484744)
![Coverage Status](./reports/coverage.svg?dummy=8484744)
![PyPI](https://img.shields.io/pypi/v/mssql_dataframe)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/jwcook23/mssql_dataframe)

Provides efficient mechanisms for updating and merging data into Transact-SQL tables from Python dataframes. This is accomplished by utilizing the fast_executemany feature of pyodbc to quickly insert into a source SQL temporary table, and then updating/merging into a target SQL table from that temporary table. Without taking into account network speed, 100,000 records can be updated/merged in a few seconds.

In practice this module may be useful for updating models, web scraping, or general data engineering tasks.

## Samples

See EXAMPLES.md for full examples.

### Initialization

``` python
import pandas as pd
pd.options.mode.chained_assignment = 'raise'

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# # connect to database using pyodbc
db = connect(database_name='master', server_name='localhost')
# # initialize the main package
sql = SQLServer(db, adjust_sql_objects=True)
```

### Updating SQL Table

UPDATE an SQL table using primary keys or other columns.

``` python
# UPDATE using dataframe's index and the SQL primary key
write.update('SomeSQLTable', dataframe[['ColumnA']])
# UPDATE using other columns by specifying match_columns
write.update('SomeSQLTable', dataframe[['ColumnA','ColumnB','ColumnC']], 
    match_columns=['ColumnB','ColumnC']
)
```

### Merging/Upsert SQL Table

MERGE (insert/update/delete) an SQL table using primary keys or other columns. Can also perform a simplier UPSERT action.

```python
## MERGE using dataframe's index and the SQL primary key
write.merge('SomeSQLTable', dataframe[['ColumnA','ColumnB']])
## MERGE using another column
write.merge('SomeSQLTable', dataframe[['ColumnA','ColumnB','ColumnC']], 
    match_columns=['ColumnC']
)
## UPSERT (if exists update, otherwise insert)
write.merge('SomeSQLTable', dataframe[['ColumnA']], delete_unmatched=False)
```

## SQL Object Creation and Modification

SQL objects will be created/modified as needed if the main class is initialized with `adjust_sql_objects=True`.

1. Tables will be created if they do not exist.
2. Column size will increase if needed, for example from TINYINT to BIGINT or VARCHAR(5) to VARCHAR(10).

Certain actions won't be taken even with `adjust_sql_objects=True` to preserve integrity.

1. A column won't change from NOT NULL automatically.
2. Column data type won't change from number like (INT, NUMERIC, etc.) to character like (VARCHAR).

Internal time tracking columns will be added (in server time) where applicable if `include_timestamps=True`, even if `adjust_sql_objects=False`.

1. `_time_insert`: a new record was inserted
2. `_time_update`: an existing record was updated

## Dependancies

[pandas](https://pandas.pydata.org/): Python DataFrames.

[pyodbc](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15): ODBC driver used for executing Transact-SQL statements.

## Installation

```cmd
pip install mssql-dataframe
```

## Contributing

See CONTRIBUTING.md

## See Also

A similiar project is [pangres](https://github.com/ThibTrip/pangres), but doesn't include SQL Server / Transact-SQL. The primary motivation for creating a new project is differences in Transact-SQL syntax, specifically MERGE in T-SQL vs UPSERT in other SQL flavors.

## Future Plans

1. merge with the open to retain deleted records in another table
2. support for decimal and numeric SQL types
