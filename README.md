# mssql_dataframe

![Tests Status](./reports/tests.svg?dummy=8484744)
![Coverage Status](./reports/coverage.svg?dummy=8484744)
![PyPI](https://img.shields.io/pypi/v/mssql_dataframe)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/jwcook23/mssql_dataframe)

A general data engineering package for Python pandas dataframes and Microsoft SQL Server / Azure SQL Database.

Provides efficient mechanisms for updating and merging from Python dataframes into SQL tables. This is accomplished by quickly inserting into a source SQL temporary table, and then updating/merging into a target SQL table from that temporary table.

## Dependancies

[pandas](https://pandas.pydata.org/): Python DataFrames.

[pyodbc](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15): ODBC driver used for executing Transact-SQL statements.

## Installation

```cmd
pip install mssql-dataframe
```

## Contributing

See CONTRIBUTING.md

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

Create a global temporary table for this demonstration. Notice a dataframe is returned with better data types assigned, and the index corresponds to the primary key.

``` python
# create a demonstration dataframe
df = pd.DataFrame({
    'ColumnA': ['1','2','3','4','5'],
    'ColumnB': ['a  .','b!','  c','d','e'],
    'ColumnC': [False, True, True, False, False]
})
df.index.name = 'PrimaryKey'

# create the table using a dataframe
df = sql.create.table_from_dataframe(
    table_name='##mssql_dataframe',
    dataframe = df,
    primary_key = 'index'
)
```

### Updating SQL Table

Update an SQL table using the primary key. Without match column details provided, the primary key / dataframe index is automatically used.

``` python
# peform a basic text cleaning task
df['ColumnB'] = df['ColumnB'].str.replace('[^\w\s]','', regex=True)
df['ColumnB'] = df['ColumnB'].str.strip()

# perform the update in SQL
updated = sql.write.update('##mssql_dataframe', df[['ColumnB']])

# read the result from SQL after the update
result = sql.read.table('##mssql_dataframe')
```

Update an SQL table using other columns that are not the primary key by specifying match columns.

``` python
# update ColumnA to 0 where ColumnC is False
sample = pd.DataFrame({
    'ColumnA': [0],
    'ColumnC': [False]
})

# peform the update in SQL
updated = sql.write.update('##mssql_dataframe', sample, match_columns='ColumnC')

# read the result from SQL after the update
result = sql.read.table('##mssql_dataframe')
```

### Merging/Upsert SQL Table

Merging the dataframe into an SQL table will:

1. Insert new records in the dataframe that are not in the SQL table.
2. Update records in the SQL table that are also in the dataframe.
3. Delete records in the SQL table not in the dataframe (if upsert=False).

```python
# read what is currenly in the table
sample = sql.read.table('##mssql_dataframe', order_column='ColumnA', order_direction='ASC')

# simulate new records
sample = sample.append(
    pd.DataFrame(
        [
            [9, 'x', True],
            [9, 'y', True],
        ], 
        columns=['ColumnA', 'ColumnB', 'ColumnC'], 
        index = pd.Index([5,6], name='PrimaryKey')
    )
)

# simulate updated records
sample.loc[sample['ColumnB'].isin(['d','e']),'ColumnA'] = 1

# simulate deleted records
sample = sample[~sample['ColumnA'].isin([2,3])]

# perform the merge
merged = sql.write.merge('##mssql_dataframe', sample)

# read the result from SQL after the merge
# records for PrimaryKey 5 & 6 have been inserted
# records for PrimaryKey 0, 3, & 4 have been updated
# records for PrimaryKey 2 & 3 have been deleted
result = sql.read.table('##mssql_dataframe')
```

Additional functionality allows data to be incrementally merged into an SQL table. This can be useful for file processing, web scraping multiple pages, or other batch processing situations.

``` python
# read what is currenly in the table
sample = sql.read.table('##mssql_dataframe', order_column='ColumnA', order_direction='ASC')

# simulate new records
sample = sample.append(
    pd.DataFrame(
        [
            [10, 'z', False],
            [10, 'z', True],
            [0, 'A', True]
        ], 
        columns=['ColumnA', 'ColumnB', 'ColumnC'], 
        index = pd.Index([7,8,9], name='PrimaryKey')
    )
)

# simulate updated records
sample.loc[sample['ColumnA']==1, 'ColumnC'] = True

# simulate deleted records
sample = sample[sample['ColumnB']!='a']
sample = sample[sample['ColumnA']!=9]

# perform the merge
merged = sql.write.merge('##mssql_dataframe', sample, delete_requires=['ColumnA'])

# read the result from SQL after the merge
# records for PrimaryKey 5 & 6 were not deleted since a value of 9 in ColumnA of the dataframe was not present
# record for PrimaryKey 0 was deleted since a value of 0 in ColumnA of the dataframe was present
# records for PrimaryKey 7 & 8 have been inserted
# records for PrimaryKey 0, 3, & 4 have been updated
result = sql.read.table('##mssql_dataframe')
```

Upsert functionality is accomplished by setting upsert=False. This results in records only being inserted or updated.

``` python
# simulate a new record
sample = sample.append(
    pd.DataFrame(
        [
            [11, 'z', False],
        ], 
        columns=['ColumnA', 'ColumnB', 'ColumnC'], 
        index = pd.Index([10], name='PrimaryKey')
    )
)

# simulate an updated record
sample.at[3,'ColumnA'] = 12

# perform the upsert
merged = sql.write.merge('##mssql_dataframe', sample, upsert=True)

# read the result from SQL after the upsert
# record for PrimaryKey 3 was updated
# record for PrimaryKey 10 was inserted
# all other records remain unchanged
result = sql.read.table('##mssql_dataframe')
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

## Future Plans

1. merge with the open to retain deleted records in another table
2. support for decimal and numeric SQL types
3. support for other SQL implementations
