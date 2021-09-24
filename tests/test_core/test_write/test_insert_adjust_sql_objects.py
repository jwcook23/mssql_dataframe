import warnings

import pytest
import pandas as pd
pd.options.mode.chained_assignment = 'raise'

from mssql_dataframe import connect
from mssql_dataframe.core import errors, create, conversion
from mssql_dataframe.core.write import insert

class package:
    def __init__(self, connection):
        self.connection = connection
        self.create = create.create(connection)
        self.insert = insert.insert(connection, adjust_sql_objects=True)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost')
    yield package(db)
    db.connection.close()


def test_insert_create_table(sql):

    table_name = '##test_insert_create_table' 

    dataframe = pd.DataFrame({
        "ColumnA": [1,2,3],
        "ColumnB": ['06/22/2021','06-22-2021','2021-06-22']
    })

    with warnings.catch_warnings(record=True) as warn:
        dataframe, schema = sql.insert.insert(table_name, dataframe=dataframe, include_timestamps=True)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating table '+table_name in str(warn[0].message)
        assert 'Created table '+table_name in str(warn[1].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[2].message)

        statement = f'SELECT * FROM {table_name}'
        result = conversion.read_values(statement, schema, sql.connection.connection)
        expected = pd.DataFrame({
            'ColumnA': pd.Series([1,2,3], dtype='UInt8'),
            'ColumnB' : pd.Series([pd.Timestamp(year=2021, month=6, day=22)]*3, dtype='datetime64[ns]')
        }).set_index(keys='ColumnA')
        assert result[expected.columns].equals(expected)
        assert all(result['_time_insert'].notna())


def test_insert_add_column(sql):

    table_name = '##test_insert_add_column'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT'
    })
    sql.connection.connection.commit()

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [2], 'ColumnC': ['zzz']})

    with warnings.catch_warnings(record=True) as warn:
        dataframe, schema = sql.insert.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert str(warn[0].message)==f'Creating column _time_insert in table {table_name} with data type DATETIME2.'
        assert str(warn[1].message)==f'Creating column ColumnB in table {table_name} with data type tinyint.'
        assert str(warn[2].message)==f'Creating column ColumnC in table {table_name} with data type varchar(3).'

        statement = f'SELECT * FROM {table_name}'
        result = conversion.read_values(statement, schema, sql.connection.connection)
        assert result[dataframe.columns].equals(dataframe)
        assert all(result['_time_insert'].notna())


def test_insert_alter_column_unchanged(sql):

    table_name = '##test_insert_alter_column_unchanged'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT',
        'ColumnB': 'VARCHAR(1)',
        'ColumnC': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': ['a'], 'ColumnC': [1]})  
    failure = errors.SQLInsufficientColumnSize('manually testing expection for ColumnB, ColumnC', ['ColumnB','ColumnC'])
    with pytest.raises(errors.SQLRecastColumnUnchanged):
        sql.insert.handle(failure, table_name, dataframe, updating_table=False)


def test_insert_alter_column_data_category(sql):

    table_name = '##test_insert_alter_column_data_category'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT',
        'ColumnB': 'VARCHAR(1)',
        'ColumnC': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [1], 'ColumnC': ['a']})  
    failure = errors.SQLInsufficientColumnSize('manually testing expection for ColumnB, ColumnC', ['ColumnB','ColumnC'])
    with pytest.raises(errors.SQLRecastColumnChangedCategory):
        sql.insert.handle(failure, table_name, dataframe, updating_table=False)


def test_insert_alter_column(sql): 

    table_name = '##test_insert_alter_column'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT',
        'ColumnB': 'VARCHAR(1)',
        'ColumnC': 'TINYINT'
    })
    sql.connection.connection.commit()

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': ['aaa'], 'ColumnC': [100000]})

    with warnings.catch_warnings(record=True) as warn:
        dataframe, schema = sql.insert.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert str(warn[0].message)==f'Creating column _time_insert in table {table_name} with data type DATETIME2.'
        assert str(warn[1].message)==f'Altering column ColumnB in table {table_name} to data type varchar(3) with is_nullable=True.'
        assert str(warn[2].message)==f'Altering column ColumnC in table {table_name} to data type int with is_nullable=True.'

        statement = f'SELECT * FROM {table_name}'
        result = conversion.read_values(statement, schema, sql.connection.connection)
        assert result[dataframe.columns].equals(dataframe)
        assert all(result['_time_insert'].notna())

        _, dtypes = conversion.sql_spec(schema, dataframe)
        assert dtypes=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'int', '_time_insert': 'datetime2'}


def test_insert_alter_primary_key(sql):

    # inital insert
    table_name = '##test_insert_alter_primary_key'
    dataframe = pd.DataFrame({
        'ColumnA': [0,1,2,3],
        'ColumnB': [0,1,2,3],
        'ColumnC': ['a','b','c','d']
    }).set_index(keys=['ColumnA','ColumnB'])
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    dataframe, schema = sql.insert.insert(table_name, dataframe, include_timestamps=False)
    _, dtypes = conversion.sql_spec(schema, dataframe)
    assert dtypes=={'ColumnA': 'tinyint', 'ColumnB': 'tinyint', 'ColumnC': 'varchar(1)'}
    assert schema.at['ColumnA','pk_seq']==1
    assert schema.at['ColumnB','pk_seq']==2
    assert pd.isna(schema.at['ColumnC','pk_seq'])

    # insert that alters primary key
    new = pd.DataFrame({
        'ColumnA': [256,257,258,259],
        'ColumnB': [4,5,6,7],
        'ColumnC': ['e','f','g','h']
    }).set_index(keys=['ColumnA','ColumnB'])
    with warnings.catch_warnings(record=True) as warn:
        new, schema = sql.insert.insert(table_name, new, include_timestamps=False)
        assert len(warn)==1
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert str(warn[0].message)=='Altering column ColumnA in table ##test_insert_alter_primary_key to data type smallint with is_nullable=False.'

        statement = f'SELECT * FROM {table_name}'
        result = conversion.read_values(statement, schema, sql.connection.connection)
        assert result.equals(dataframe.append(new))
        _, dtypes = conversion.sql_spec(schema, new)
        assert dtypes=={'ColumnA': 'smallint', 'ColumnB': 'tinyint', 'ColumnC': 'varchar(1)'}
        assert schema.at['ColumnA','pk_seq']==1
        assert schema.at['ColumnB','pk_seq']==2
        assert pd.isna(schema.at['ColumnC','pk_seq'])


def test_insert_add_and_alter_column(sql):

    table_name = '##test_insert_add_and_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [0,1,2,3],
        'ColumnB': [0,1,2,3]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        sql.connection.connection.commit()
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)

    dataframe['ColumnB'] = [256,257,258,259]
    dataframe['ColumnC'] = [0,1,2,3]
    with warnings.catch_warnings(record=True) as warn:
        dataframe, schema = sql.insert.insert(table_name, dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert str(warn[0].message)==f'Creating column _time_insert in table {table_name} with data type DATETIME2.'        
        assert str(warn[1].message)==f'Creating column ColumnC in table {table_name} with data type tinyint.'
        assert str(warn[2].message)==f'Altering column ColumnB in table {table_name} to data type smallint with is_nullable=False.'
        
        statement = f'SELECT * FROM {table_name}'
        result = conversion.read_values(statement, schema, sql.connection.connection)
        assert result[dataframe.columns].equals(dataframe)
        assert all(result['_time_insert'].notna())

        _, dtypes = conversion.sql_spec(schema, dataframe)
        assert dtypes=={'_index': 'tinyint', 'ColumnA': 'tinyint', 'ColumnB': 'smallint', '_time_insert': 'datetime2', 'ColumnC': 'tinyint'}