# mssql_dataframe

![Tests Status](./reports/tests.svg?dummy=8484744)
![Coverage Status](./reports/coverage.svg?dummy=8484744)
![PyPI](https://img.shields.io/pypi/v/mssql_dataframe)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/jwcook23/mssql_dataframe)

A general data engineering package for Python pandas dataframes and Microsoft SQL Server / Azure SQL Database.

Provides efficient mechanisms for updating and merging from Python dataframes into SQL tables. This is accomplished by utilizing the fast_executemany feature of pyodbc to quickly insert into a source SQL temporary table, and then updating/merging into a target SQL table from that temporary table.

## Samples

See TUTORIAL.md for a full example.

### Initialization

``` python
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

# connect to database using pyodbc
db = connect(database_name='master', server_name='localhost')
# initialize the main mssql_dataframe package
sql = SQLServer(db)
```

### Create Sample Table

Create a global temporary table for this demonstration.

``` python
# create a demonstration dataframe
df = pd.DataFrame({
    'ColumnA': ['1','2','3','4','5'],
    'ColumnB': ['a  .','b!','  c','d','e'],
    'ColumnC': [False, True, True, False, False]
})
df.index.name = 'PrimaryKey'

# create the table using a dataframe, notice a dataframe is returned with better data types assigned
df = sql.create.table_from_dataframe(
    table_name='##mssql_dataframe',
    dataframe = df,
    primary_key = 'index'
)
```

### Updating SQL Table

Update an SQL table using the primary key.

``` python
# peform a basic text cleaning task
df['ColumnB'] = df['ColumnB'].str.replace('[^\w\s]','')
df['ColumnB'] = df['ColumnB'].str.strip()

# perform the update in SQL
df = sql.write.update[['ColumnB']]

# read the result from SQL after the update
result = sql.read.table('##mssql_dataframe')
```

Update an SQL table using other columns that are not the primary key.

``` python
# update ColumnA to 0 where ColumnC is False
sample = pd.DataFrame({
    'ColumnA': [0],
    'ColumnC': [False]
})

# peform the update in SQL
sample = sql.write.update('##mssql_dataframe', sample, match_columns='ColumnC')

# read the result from SQL after the update
result = sql.read.table('##mssql_dataframe')
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

SQL objects will be created/modified as needed if the main class is initialized with `autoadjust_sql_objects=True`.

1. Tables will be created if they do not exist.
2. Column size will increase if needed, for example from TINYINT to BIGINT or VARCHAR(5) to VARCHAR(10).

Certain actions won't be taken even with `autoadjust_sql_objects=True` to preserve integrity.

1. A column won't change from NOT NULL automatically.
2. Column data type won't change from number like (INT, NUMERIC, etc.) to character like (VARCHAR).

Internal time tracking columns will be added (in server time) where applicable if `include_timestamps=True`, even if `autoadjust_sql_objects=False`.

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
3. support for other SQL implementations
