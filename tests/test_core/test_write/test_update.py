import warnings

import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors, helpers


@pytest.fixture(scope="module")
def sql():
    connection = connect.connect(database_name='tempdb', server_name='localhost')
    yield SQLServer(connection, adjust_sql_objects=False)
    connection.connection.close()


@pytest.fixture(scope="module")
def sql_adjustable():
    connection = connect.connect(database_name='tempdb', server_name='localhost')
    with warnings.catch_warnings(record=True) as warn:
        yield SQLServer(connection, adjust_sql_objects=True)
        connection.connection.close()
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert 'SQL objects will be created/modified as needed' in str(warn[-1].message)


def test_update_errors(sql):

    table_name = '##test_update_errors'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'VARCHAR(1)'
    })
 
    with pytest.raises(errors.SQLTableDoesNotExist):
        sql.write.update('error'+table_name, dataframe=pd.DataFrame({'ColumnA': [1]}))

    with pytest.raises(errors.SQLColumnDoesNotExist):
        sql.write.update(table_name, dataframe=pd.DataFrame({'ColumnA': [0],'ColumnC': [1]}), match_columns=['ColumnA'])

    with pytest.raises(errors.SQLInsufficientColumnSize):
        sql.write.update(table_name, dataframe=pd.DataFrame({'ColumnA': [100000],'ColumnB': ['aaa']}), match_columns=['ColumnA'])


def test_update_one_match_column(sql):

    table_name = '##test_update_one_match_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe['ColumnC'] = [5,6]
    with warnings.catch_warnings(record=True) as warn:
        sql.write.update(table_name, dataframe[['ColumnC']])
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_update' in str(warn[0].message)
    result = sql.read.select(table_name)
    assert all(result[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
    assert all(result['_time_update'].notna())
    assert '_time_insert' not in result.columns


def test_update_two_match_columns(sql):

    table_name = '##test_update_two_match_columns'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='sql')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the primary key created in SQL and ColumnA
    dataframe = sql.read.select(table_name)
    dataframe['ColumnC'] = [5,6]
    with warnings.catch_warnings(record=True) as warn:
        sql.write.update(table_name, dataframe, match_columns=['_pk','ColumnA'])
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_update' in str(warn[0].message)
    result = sql.read.select(table_name)
    assert all(result[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
    assert all(result['_time_update'].notna())
    assert '_time_insert' not in result.columns


def test_update_composite_pk(sql):

    table_name = '##test_update_composite_pk'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    dataframe = dataframe.set_index(keys=['ColumnA','ColumnB'])
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the primary key created in SQL and ColumnA
    dataframe['ColumnC'] = [5,6]
    sql.write.update(table_name, dataframe, include_timestamps=False)
    result = sql.read.select(table_name)
    assert all(result[['ColumnC']]==dataframe[['ColumnC']])


def test_update_exclude_timestamps(sql):

    table_name = '##test_update_exclude_timestamps'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe['ColumnC'] = [5,6]
    sql.write.update(table_name, dataframe[['ColumnC']], include_timestamps=False)
    result = sql.read.select(table_name)
    assert all(result[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
    assert '_time_update' not in result.columns
    assert '_time_insert' not in result.columns


def test_update_create_table(sql_adjustable):

    table_name = '##test_update_create_table'

    # create table to update
    dataframe = pd.DataFrame({
        '_pk': [0,1],
        'ColumnA': [1,2]
    }).set_index(keys='_pk')
    
    with pytest.raises(errors.SQLTableDoesNotExist):
        sql_adjustable.write.update(table_name, dataframe)


def test_update_add_column(sql_adjustable):
    
    table_name = '##test_update_add_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

    # update using the SQL primary key that came from the dataframe's index
    dataframe['NewColumn'] = [3,4]
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.update(table_name, dataframe[['NewColumn']])
        assert len(warn)==2
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[1].message)
        result = sql_adjustable.read.select(table_name)
        assert all(result[['ColumnA','NewColumn']]==dataframe)
        assert (result['_time_update'].notna()).all()


def test_update_alter_column(sql_adjustable):

    table_name = '##test_update_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [0,0]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key=None)
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

    # update using ColumnA
    dataframe['ColumnB'] = ['aaa','bbb']
    dataframe['ColumnC'] = [256, 256]
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.update(table_name, dataframe, match_columns=['ColumnA'])
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_update in table '+table_name in str(warn[0].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[1].message)
        assert 'Altering column ColumnC in table '+table_name in str(warn[2].message)
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_update'].notna())

    schema = helpers.get_schema(sql_adjustable.connection, table_name)
    columns,_,_,_ = helpers.flatten_schema(schema)
    assert columns=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'smallint', '_time_update': 'datetime'}


def test_update_add_and_alter_column(sql_adjustable):

    table_name = '##test_update_add_and_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b']
    })
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

    # update using the SQL primary key that came from the dataframe's index
    dataframe['ColumnB'] = ['aaa','bbb']
    dataframe['NewColumn'] = [3,4]
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.update(table_name, dataframe[['ColumnB', 'NewColumn']])
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[1].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[2].message)
        result = sql_adjustable.read.select(table_name)
        assert all(result[['ColumnA','ColumnB','NewColumn']]==dataframe)
        assert (result['_time_update'].notna()).all()