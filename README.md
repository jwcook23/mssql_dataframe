# mssql_dataframe

![Test Status](https://github.com/jwcook23/mssql_dataframe/blob/main/reports/tests.svg?raw=true)
![Coverage Status](https://github.com/jwcook23/mssql_dataframe/blob/main/reports/coverage.svg?raw=true)
![Flake8 Status](https://github.com/jwcook23/mssql_dataframe/blob/main/reports/flake8.svg?raw=true)
[![Bandit Security](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)
![PyPI](https://img.shields.io/pypi/v/mssql_dataframe)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Build Status](https://dev.azure.com/jasoncook1989/mssql_dataframe/_apis/build/status/continuous-delivery?branchName=main)](https://dev.azure.com/jasoncook1989/mssql_dataframe/_build/latest?definitionId=2&branchName=main)
[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/jwcook23/mssql_dataframe)

A data engineering package for Python pandas dataframes and Microsoft Transact-SQL. It provides more advanced methods for writting dataframes including update, merge, upsert.

1. Update: updates records in SQL table
2. Upsert: insert or update records in SQL table
3. Merge: update, insert, or delete records in SQL table

These more advanced methods are designed to provide more funcationality than is offered by the pandas.DataFrame.to_sql method. pandas.DataFrame.to_sql offers 3 options if the SQL table already exists with the parameter `if_exists={'fail', 'replace', 'append'}`.

See [QUICKSTART](QUICKSTART.md) for a full overview of functionality.

## Initialization and Sample SQL Table

<!--phmdoctest-setup-->
``` python
import env
import pandas as pd
from mssql_dataframe import SQLServer

# connect to database using pyodbc
sql = SQLServer(database=env.database, server=env.server)

# create a demonstration dataframe
df = pd.DataFrame({
    'ColumnA': ['1','2','3','4','5'],
    'ColumnB': ['a  .','b!','  c','d','e'],
}, index=pd.Index([0, 1, 2, 3, 4], name='PK'))

# create the table using a dataframe
df = sql.create.table_from_dataframe(
    table_name='##mssql_dataframe_readme',
    dataframe = df,
    primary_key = 'index'
)
```

## Update

Records in an SQL table are updated by simply providing a dataframe. By default a match on the SQL table's primary key is required for a record to be updated.

```python
# update records for index 0 & 1
update_df = pd.DataFrame({
        'ColumnA': ['11','22'],
        'ColumnB': ['A','B'],
}, index=pd.Index([0, 1], name='PK'))
# update data in the SQL table
update_df = sql.write.update('##mssql_dataframe_readme', update_df)
```

## Merge

Records can be inserted/updated/deleted by providing a dataframe to the merge method. Again the primary key in the SQL table is used by default.

1. dataframe column value doesn't match SQL column value -> insert record into SQL
2. dataframe column value matches SQL column value -> update record in SQL
3. SQL column value not in dataframe column -> delete record in SQL

```python
# update existing record for index 0
# insert new record for index 5
# delete missing records for index 1,2,3,4
merge_df = pd.DataFrame({
        'ColumnA': ['11','6'],
        'ColumnB': ['aa','f'],
}, index=pd.Index([0, 6], name='PK'))
# merge data in the SQL table
merged_df = sql.write.merge('##mssql_dataframe_readme', merge_df)
```

## Upsert

The merge method can be restricted to not delete records in SQL by specifying the upsert flag. Records in SQL are then only inserted or updated.

```python
# update existing record for index 0
# insert new record for index 7
# records not in the dataframe but in SQL won't be deleted
upsert_df = pd.DataFrame({
        'ColumnA': ['11','7'],
        'ColumnB': ['AA','g'],
}, index=pd.Index([0, 7], name='PK'))
sql.write.merge('##mssql_dataframe_readme', upsert_df, upsert=True)
```

## Installation

```cmd
pip install mssql-dataframe
```

## Dependancies

[pandas](https://pandas.pydata.org/): Python DataFrames.

[pyodbc](https://github.com/mkleehammer/pyodbc/wiki/): ODBC driver used for executing Transact-SQL statements.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md).
