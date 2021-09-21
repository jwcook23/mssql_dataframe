import warnings

import pytest
import pandas as pd
pd.options.mode.chained_assignment = 'raise'

from mssql_dataframe import connect
from mssql_dataframe.core import errors, create, conversion
from mssql_dataframe.core.write import update

class package:
    def __init__(self, connection):
        self.connection = connection
        self.create = create.create(connection)
        self.update = update.update(connection)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name='tempdb', server_name='localhost')
    yield package(db)
    db.connection.close()


def test_update_errors(sql):

    table_name = '##test_update_errors'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'VARCHAR(1)'
    })
    sql.connection.connection.commit()
 
    with pytest.raises(errors.SQLTableDoesNotExist):
        sql.update.update('error'+table_name, dataframe=pd.DataFrame({'ColumnA': [1]}))

    with pytest.raises(errors.SQLColumnDoesNotExist):
        sql.update.update(table_name, dataframe=pd.DataFrame({'ColumnA': [0],'ColumnC': [1]}), match_columns=['ColumnA'])

    with pytest.raises(errors.SQLInsufficientColumnSize):
        sql.update.update(table_name, dataframe=pd.DataFrame({'ColumnA': [100000],'ColumnB': ['aaa']}), match_columns=['ColumnA'])


def test_update_primary_key(sql):

    table_name = '##test_update_primary_key'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    dataframe, _ = sql.update.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe['ColumnC'] = [5,6]
    updated, schema = sql.update.update(table_name, dataframe=dataframe[['ColumnC']], include_timestamps=False)
    dataframe['ColumnC'] = updated['ColumnC']

    # test result
    statement = f'SELECT * FROM {table_name}'
    result = conversion.read_values(statement, schema, sql.connection.connection)
    assert dataframe.equals(result[dataframe.columns])
    assert '_time_update' not in result.columns
    assert '_time_insert' not in result.columns


def test_update_nonpk_column(sql):

    table_name = '##test_update_nonpk_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    }).set_index(keys='ColumnA')
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    dataframe, _ = sql.update.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe['ColumnB'] = ['c','d']
    updated, schema = sql.update.update(table_name, dataframe=dataframe[['ColumnB','ColumnC']],
        match_columns=['ColumnC'], include_timestamps=False)
    dataframe['ColumnB'] = updated['ColumnB']

    # test result
    statement = f'SELECT * FROM {table_name}'
    result = conversion.read_values(statement, schema, sql.connection.connection)
    assert dataframe.equals(result[dataframe.columns])
    assert '_time_update' not in result.columns
    assert '_time_insert' not in result.columns


def test_update_two_match_columns(sql):

    table_name = '##test_update_two_match_columns'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='sql')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    dataframe, schema = sql.update.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the primary key created in SQL and ColumnA
    dataframe = conversion.read_values(f'SELECT * FROM {table_name}', schema, sql.connection.connection)
    dataframe['ColumnC'] = [5,6]
    with warnings.catch_warnings(record=True) as warn:
        updated, schema = sql.update.update(table_name, dataframe, match_columns=['_pk','ColumnA'])
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert str(warn[0].message)=='Creating column _time_update in table ##test_update_two_match_columns with data type DATETIME2.'

    # test result
    statement = f'SELECT * FROM {table_name}'
    result = conversion.read_values(statement, schema, sql.connection.connection)
    assert updated.equals(result[updated.columns])
    assert result['_time_update'].notna().all()


def test_update_composite_pk(sql):

    table_name = '##test_update_composite_pk'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    dataframe = dataframe.set_index(keys=['ColumnA','ColumnB'])
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    dataframe, _ = sql.update.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the primary key created in SQL and ColumnA
    dataframe['ColumnC'] = [5,6]
    updated, schema = sql.update.update(table_name, dataframe, include_timestamps=False)

    # test result
    statement = f'SELECT * FROM {table_name}'
    result = conversion.read_values(statement, schema, sql.connection.connection)
    assert result.equals(updated)