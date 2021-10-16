# mssql_dataframe

![Tests Status](./reports/tests.svg?dummy=8484744)
![Coverage Status](./reports/coverage.svg?dummy=8484744)
![PyPI](https://img.shields.io/pypi/v/mssql_dataframe)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[![Open in Visual Studio Code](https://open.vscode.dev/badges/open-in-vscode.svg)](https://open.vscode.dev/jwcook23/mssql_dataframe)

A data engineering package for Python pandas dataframes and Microsoft SQL Server / Azure SQL Database.

Provides efficient mechanisms for updating and merging from Python dataframes into SQL tables. This is accomplished by quickly inserting into a source SQL temporary table, and then updating/merging into a target SQL table from that temporary table.

## Dependancies

[pandas](https://pandas.pydata.org/): Python DataFrames.

[pyodbc](https://github.com/mkleehammer/pyodbc/wiki/): ODBC driver used for executing Transact-SQL statements.

## Installation

```cmd
pip install mssql-dataframe
```

## Quick Start

### Initialization

``` python
import pandas as pd
from mssql_dataframe import SQLServer

# connect to database using pyodbc
sql = SQLServer(database='master', server='localhost')
```

### Create Sample Table

Create a global temporary table for demonstration purposes. Notice a dataframe is returned with better data types assigned, and the index corresponds to the primary key.

``` python
# create a demonstration dataframe
df = pd.DataFrame({
    'ColumnA': ['1','2','3','4','5'],
    'ColumnB': ['a  .','b!','  c','d','e'],
    'ColumnC': [False, True, True, False, False]
}, index=pd.Index([0, 1, 2, 3, 4], name='PrimaryKey'))

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

## Additional Functionality

### include_metadata_timestamps

If mssql_dataframe is initialized with include_metadata_timestamps=True insert, update, and merge operations will include columns detailing when records are inserted or updated. These are timestamps in server time.

``` python
# intialized with flag to include metadata timestamps
sql = SQLServer(include_metadata_timestamps=True)

# create sample table
df = pd.DataFrame({
    'ColumnA': ['1','2','3','4','5'],
}, index=pd.Index([0, 1, 2, 3, 4], name='PrimaryKey'))

df = sql.create.table_from_dataframe(
    table_name='##mssql_metadata',
    dataframe = df,
    primary_key = 'index'
)

# all records have a _time_insert value
result = sql.read.table('##mssql_metadata')

# simulate an updated record
result.at[0,'ColumnA'] = 9
updated = sql.write.update('##mssql_metadata', result.loc[[0]])

# record 0 now has a _time_update value
# the _time_update column was automatically created
result = sql.read.table('##mssql_metadata')
```

### Manual SQL Column Modification

mssql_dataframe contains methods to adjust SQL columns.

``` python
import pandas as pd
from mssql_dataframe import SQLServer

sql = SQLServer()

# create sample table
df = pd.DataFrame({
    'ColumnA': ['1','2','3','4','5'],
}, index=pd.Index([0, 1, 2, 3, 4], name='PrimaryKey'))

df = sql.create.table_from_dataframe(
    table_name='##mssql_modify',
    dataframe = df,
    primary_key = 'index'
)

# modify ColumnA
sql.modify.column('##mssql_modify', 'alter', 'ColumnA', 'BIGINT', is_nullable=True)

# notice ColumnA is now BIGINT and nullable
schema = sql.get_schema('##mssql_modify')
```

### Automatic SQL Object Creation and Modification

SQL objects will be created/modified as needed if the class is initialized with `autoadjust_sql_objects=True`.

1. Tables will be created if they do not exist.
2. Column size will increase if needed, for example from TINYINT to BIGINT or VARCHAR(5) to VARCHAR(10).

``` python
import pandas as pd
from mssql_dataframe import SQLServer

sql = SQLServer(autoadjust_sql_objects=True)

# sample dataframe
df = pd.DataFrame({
    'ColumnA': [1,2,3,4,5],
}, index=pd.Index([0, 1, 2, 3, 4], name='PrimaryKey'))

# create table by inserting into a table that doesn't exist
df = sql.write.insert('##mssql_auto', df)

# automatically add a column
new = pd.DataFrame({
    'ColumnA': [6],
    'ColumnB' : ['z']
}, index=pd.Index([5], name='PrimaryKey'))
new = sql.write.insert('##mssql_auto', new)

# automatically modify columns
alter = pd.DataFrame({
    'ColumnA': [1000],
    'ColumnB' : ['zzz']
}, index=pd.Index([6], name='PrimaryKey'))
alter = sql.write.insert('##mssql_auto', alter)

# prevent  automatically modifying to different category
error = pd.DataFrame({
    'ColumnA': ['z'],
}, index=pd.Index([7], name='PrimaryKey'))
try:
    error = sql.write.insert('##mssql_auto', error)
except sql.exceptions.DataframeColumnInvalidValue:
    print('ColumnA not changed to string like column.')
```

## Contributing

See CONTRIBUTING.md
