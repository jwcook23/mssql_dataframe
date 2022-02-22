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

These more advanced methods are designed to provide more funcationality than is offered by the pandas.DataFrame.to_sql method.

```python
# pandas.DataFrame.to_sql method if the table already exists
pandas.DataFrame.to_sql(name, con, if_exists='fail')

# if_exists : {‘fail’, ‘replace’, ‘append’}, default ‘fail’
# 'fail': Raise a ValueError.
# 'replace': Drop the table before inserting new values.
# 'append': Insert new values to the existing table.
```

See [QUICKSTART](QUICKSTART.md) for a full overview of functionality.

## Update

Records in an SQL table are updated by simply providing a dataframe. By default a match on the SQL table's primary key is required for a record to be updated.

```python
mssql.write.update(table_name, dataframe)
```

## Merge

Records can be inserted/updated/deleted by providing a dataframe to the merge method. Again the primary key in the SQL table is used by default.

1. dataframe column value doesn't match SQL column value -> insert record into SQL
2. dataframe column value matches SQL column value -> update record in SQL
3. SQL column value not in dataframe column -> delete record in SQL

```python
sql.write.merge(table_name, dataframe)
```

## Upsert

The merge method can be restricted to not delete records in SQL by specifying the upsert flag. Records in SQL are then only inserted or updated.

```python
sql.write.merge(table_name, dataframe, upsert=True)
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
