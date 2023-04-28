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
```

## Update

Records in an SQL table are updated by simply providing a dataframe. By default a match on the SQL table's primary key is required for a record to be updated.

```python
# create demo SQL table
df = sql.create.table(
    table_name = '##mssql_update',
    columns = {'Column1': 'VARCHAR(10)', 'Column2': 'TINYINT', 'PK': 'CHAR(1)'},
    primary_key_column = 'PK'
)

# create a demo dataframe
df = pd.DataFrame({
    'Column1': ['A_Initial', 'B_Initial'],
    'Column2': [1, 2],
}, index=pd.Index(['A', 'B'], name='PK'))

# perform an initial insert
sql.write.insert('##mssql_update', df)

# update records
update_df = pd.DataFrame({
        'Column1': ['A_Updated'],
}, index=pd.Index(['A'], name='PK'))
# update data in the SQL table
update_df = sql.write.update('##mssql_update', update_df)

# validate the result
result = sql.read.table('##mssql_update')
assert result.at['A', 'Column1'] == 'A_Updated'
assert result.at['A', 'Column2'] == 1
assert result.at['B', 'Column1'] == 'B_Initial'
assert result.at['B', 'Column2'] == 2
```

## Merge

Records can be inserted/updated/deleted by providing a dataframe to the merge method. Again the primary key in the SQL table is used by default.

1. dataframe column value doesn't match SQL column value -> insert record into SQL
2. dataframe column value matches SQL column value -> update record in SQL
3. SQL column value not in dataframe column -> delete record in SQL

```python
# create demo SQL table
df = sql.create.table(
    table_name = '##mssql_merge',
    columns = {'Column1': 'VARCHAR(10)', 'Column2': 'TINYINT', 'PK': 'CHAR(1)'},
    primary_key_column = 'PK'
)

# create a demo dataframe
df = pd.DataFrame({
    'Column1': ['A_Initial', 'B_Initial'],
    'Column2': [1, 2],
}, index=pd.Index(['A', 'B'], name='PK'))

# perform an initial insert
sql.write.insert('##mssql_merge', df)

# perform merge
sql.write.merge(
        '##mssql_merge',
        pd.DataFrame.from_records([
                {'Column1': 'C_New', 'Column2': 3, 'PK': 'C'},
                {'Column1': 'B_Updated', 'Column2': 0, 'PK': 'B'},
        ]).set_index('PK')
)

# validate the results
result = sql.read.table('##mssql_merge')
assert 'A' not in result.index
assert result.at['C', 'Column1'] == 'C_New'
assert result.at['B', 'Column1'] == 'B_Updated'
assert result.at['B', 'Column2'] == 0
```

## Upsert

The merge method can be restricted to not delete records in SQL by specifying the upsert flag. Records in SQL are then only inserted or updated.

```python
# create demo SQL table
df = sql.create.table(
    table_name = '##mssql_upsert',
    columns = {'Column1': 'VARCHAR(10)', 'Column2': 'TINYINT', 'PK': 'CHAR(1)'},
    primary_key_column = 'PK'
)

# create a demo dataframe
df = pd.DataFrame({
    'Column1': ['A_Initial', 'B_Initial'],
    'Column2': [1, 2],
}, index=pd.Index(['A', 'B'], name='PK'))

# perform an initial insert
sql.write.insert('##mssql_upsert', df)

# perform upsert
sql.write.merge(
        '##mssql_upsert',
        pd.DataFrame.from_records([
                {'Column1': 'C_New', 'Column2': 3, 'PK': 'C'},
                {'Column1': 'B_Updated', 'Column2': 0, 'PK': 'B'},
        ]).set_index('PK'),
        upsert = True
)

# validate the results
result = sql.read.table('##mssql_upsert')
assert result.at['A', 'Column1'] == 'A_Initial'
assert result.at['A', 'Column2'] == 1
assert result.at['C', 'Column1'] == 'C_New'
assert result.at['B', 'Column1'] == 'B_Updated'
assert result.at['B', 'Column2'] == 0
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
