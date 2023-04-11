# Quick Start

## Initialization and Sample SQL Table

Create a connection to SQL Server.

<!--phmdoctest-setup-->
``` python
import env
import pandas as pd
from mssql_dataframe import SQLServer

# connect to database using pyodbc
sql = SQLServer(database=env.database, server=env.server)
```

## Updating SQL Table

Update an SQL table using the primary key. Without match column details provided, the primary key / dataframe index is automatically used.

``` python
# create demo dataframe
df = pd.DataFrame(
    {
        'Desc': ['A_Initial', 'B_Initial', 'C_Initial'],
        'OtherCol': [0, 1, 2],
    }, index=pd.Series(['A', 'B', 'C'], name='PK')
)

# create demo table in SQL
sql.create.table(
    table_name='##_update',
    columns={'Desc': 'VARCHAR(10)', 'OtherCol': 'TINYINT', 'PK': 'CHAR(1)'},
    primary_key_column='PK'
)

# insert data into demo table
df = sql.write.insert('##_update', df)

# update Row0 using the dataframe index
sample = pd.DataFrame({'Desc': 'A_Updated'}, index=pd.Series(['A'], name='PK'))
sql.write.update('##_update', sample)

# verify the update in SQL
result = sql.read.table('##_update')

# Desc for PK 0 has been updated
assert result.at['A','Desc'] == 'A_Updated'

# OtherCol for PK 0 remains unchanged despite not being in sample
assert result.at['A', 'OtherCol'] == df.at['A', 'OtherCol']

# records not in sample remain unchanged
assert result.at['B', 'Desc'] == 'B_Initial'
assert result.at['C', 'Desc'] == 'C_Initial'
assert result.loc[['B','C']].equals(df.loc[['B','C']])
```

Update an SQL table without using the dataframe's index / SQL primary key.

``` python
# create demo dataframe
df = pd.DataFrame(
    {
        'Desc': ['A_Initial', 'B_Initial', 'C_Initial'],
        'OtherCol': [0, 1, 2],
    }
)

# create demo table in SQL
sql.create.table(
    table_name='##_update_nopk',
    columns={'Desc': 'VARCHAR(10)', 'OtherCol': 'TINYINT'}
)

# insert data into demo table
df = sql.write.insert('##_update_nopk', df)

# update B using OtherCol
sample = pd.DataFrame({'Desc': ['B_Updated'], 'OtherCol': [1]})
sql.write.update('##_update_nopk', sample, match_columns='OtherCol')

# verify the update in SQL
result = sql.read.table('##_update_nopk')

# Desc where OtherCol == 1 has been updated
assert (result.loc[result['OtherCol'] == 1, 'Desc'] == 'B_Updated').all()

# records not in sample remain unchanged
assert (result.loc[result['OtherCol'] != 1, 'Desc'] == ['A_Initial','C_Initial']).all()
```

## Merging/Upsert SQL Table

Merging the dataframe into an SQL table will:

1. Insert new records in the dataframe that are not in the SQL table.
2. Update records in the SQL table that are also in the dataframe.
3. Delete records in the SQL table not in the dataframe (if upsert=False which is default).

```python
# create demo dataframe
df = pd.DataFrame(
    {
        'Desc': ['A_Initial', 'B_Initial', 'C_Initial'],
        'OtherCol': [0, 1, 2],
    },
    index=pd.Index(['A', 'B', 'C'], name='PK')
)

# create demo table in SQL
sql.create.table(
    table_name='##_merge',
    columns={'Desc': 'VARCHAR(10)', 'OtherCol': 'TINYINT', 'PK': 'CHAR(1)'},
    primary_key_column = 'PK'
)

# insert data into demo table
df = sql.write.insert('##_merge', df)

# perform the merge
sql.write.merge(
    '##_merge',
    pd.DataFrame.from_records([
        # new record D
        {'Desc': 'D_New', 'OtherCol': 4, 'PK': 'D'},
        # updated record A
        {'Desc': 'A_Updated', 'PK': 'A'},
        # B isn't included
        # C isn't included
    ]).set_index('PK')
)

# verify the merge in SQL
result = sql.read.table('##_merge')

assert result.at['A', 'Desc'] == 'A_Updated'
assert result.at['D', 'Desc'] == 'D_New'
assert 'B' not in result.index
assert 'C' not in result.index
```

Additional functionality allows data to be incrementally merged into an SQL table. This can be used for batch operations. It prevents records from being deleted if certain conditions aren't met.

``` python
# create demo dataframe
df = pd.DataFrame(
    {
        'Desc': ['A_Initial', 'B_Initial', 'C_Initial', 'D_Initial'],
        'OtherCol': [0, 1, 2, 2],
    },
    index=pd.Index(['A', 'B', 'C', 'D'], name='PK')
)

# create demo table in SQL
sql.create.table(
    table_name='##_merge_batch',
    columns={'Desc': 'VARCHAR(10)', 'OtherCol': 'TINYINT', 'PK': 'CHAR(1)'},
    primary_key_column = 'PK'
)

# insert data into demo table
df = sql.write.insert('##_merge_batch', df)

# perform the incremental merge
sql.write.merge(
    '##_merge_batch',
    pd.DataFrame.from_records([
        # new record
        {'Desc': 'E_New', 'OtherCol': 4, 'PK': 'E'},
        # updated record
        {'Desc': 'A_Updated', 'OtherCol': 0, 'PK': 'A'},
        # update record, note OtherCol = 2
        {'Desc': 'C_Updated', 'OtherCol': 2, 'PK': 'C'},
        # B isn't included
        # D isn't included
    ]).set_index('PK'),
    delete_requires=['OtherCol']
)

# verify the incremental merge in SQL
result = sql.read.table('##_merge_batch')

# B remains since a value of 1 wasn't included in OtherCol
assert result.at['B', 'Desc'] == 'B_Initial'

# D was deleted since a value of 2 was included in OtherCol
assert 'D' not in result.index

# straightforward insert and updates
assert result.at['A', 'Desc'] == 'A_Updated'
assert result.at['C', 'Desc'] == 'C_Updated'
assert result.at['E', 'Desc'] == 'E_New'
```

Upsert functionality is accomplished by setting upsert=False. This results in records only being inserted or updated.

``` python
# create demo dataframe
df = pd.DataFrame(
    {
        'Desc': ['A_Initial', 'B_Initial'],
        'OtherCol': [0, 1],
    },
    index=pd.Index(['A', 'B'], name='PK')
)

# create demo table in SQL
sql.create.table(
    table_name='##_upsert',
    columns={'Desc': 'VARCHAR(10)', 'OtherCol': 'TINYINT', 'PK': 'CHAR(1)'},
    primary_key_column = 'PK'
)

# insert data into demo table
df = sql.write.insert('##_upsert', df)

# perform the upsert
sql.write.merge(
    '##_upsert',
    pd.DataFrame.from_records([
        # new record
        {'Desc': 'C_New', 'PK': 'C'},
        # updated record
        {'Desc': 'B_Updated', 'OtherCol': 2, 'PK': 'B'}
    ]).set_index('PK'),
    upsert = True
)

# verify the upsert in SQL
result = sql.read.table('##_upsert')

# A remains
assert result.at['A', 'Desc'] == 'A_Initial'

# B is updated
assert result.at['B', 'Desc'] == 'B_Updated'

# C is inserted
assert result.at['C', 'Desc'] == 'C_New'
```

## Additional Functionality

### include_metadata_timestamps

If mssql_dataframe is initialized with include_metadata_timestamps=True insert, update, and merge operations will include columns detailing when records are inserted or updated. These are timestamps in server time.

``` python
# intialized with flag to include metadata timestamps
sql = SQLServer(database=env.database, server=env.server, include_metadata_timestamps=True)

# create table in SQL
sql.create.table(
    table_name='##_metadata',
    columns = {'Desc': 'VARCHAR(10)', 'PK': 'CHAR(1)'},
    primary_key_column = 'PK'
)

# insert initial data
df = pd.DataFrame({
    'Desc': ['A_Initial', 'B_Initial'],
}, index=pd.Index(['A', 'B'], name='PK'))

sql.write.insert('##_metadata', df)

# all records have a _time_insert value
result = sql.read.table('##_metadata')
assert result['_time_insert'].notna().all()

# update a record and verify _time_update value
sql.write.update(
    '##_metadata',
    pd.DataFrame({'Desc': 'B_Updated'}, index=pd.Series(['B'], name='PK'))
)
result = sql.read.table('##_metadata')
assert result.at['B', 'Desc'] == 'B_Updated'
assert pd.notna(result.at['B', '_time_update'])
assert result.at['A', 'Desc'] == 'A_Initial'
assert pd.isna(result.at['A', '_time_update'])
```

### Manual SQL Column Modification

mssql_dataframe contains methods to adjust SQL columns.

``` python
import pandas as pd
from mssql_dataframe import SQLServer

sql = SQLServer(database=env.database, server=env.server)

# create sample table
df = pd.DataFrame({
    'ColumnA': [1, 2, 3, 4, 5],
}, index=pd.Index([0, 1, 2, 3, 4], name='PrimaryKey'))


sql.create.table(
    table_name='##_modify',
    columns = {'ColumnA': 'TINYINT', 'PrimaryKey': 'TINYINT'},
    primary_key_column = 'PrimaryKey',
    not_nullable = 'ColumnA'
)

# modify ColumnA
sql.modify.column('##_modify', 'alter', 'ColumnA', 'BIGINT', is_nullable=True)

# validate schema has changed
schema = sql.get_schema('##_modify')
assert schema.at['ColumnA', 'sql_type'] == 'bigint'
assert schema.at['ColumnA', 'is_nullable']
```
