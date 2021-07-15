from datetime import date
import warnings

import pytest
import pandas as pd
import numpy as np

from mssql_dataframe import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors, helpers


@pytest.fixture(scope="module")
def sql():
    connection = connect.connect(database_name='tempdb', server_name='localhost', autocommit=False)
    with warnings.catch_warnings(record=True) as warn:
        yield SQLServer(connection, adjust_sql_objects=True)
        connection.connection.close()
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert 'SQL objects will be created/modified as needed' in str(warn[-1].message)


def test_insert_create_table(sql):

    table_name = '##test_insert_create_table'

    dataframe = pd.DataFrame({
        "ColumnA": [1,2]
    })

    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe=dataframe)
        assert len(warn)==2
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating table '+table_name in str(warn[0].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[1].message)        
        results = sql.read.select(table_name)
        assert all(results[['ColumnA']]==dataframe[['ColumnA']])
        assert all(results['_time_insert'].notna())


def test_insert_add_column(sql):

    table_name = '##test_insert_add_column'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [2], 'ColumnC': [3]})

    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)        
        assert 'Creating column ColumnB in table '+table_name in str(warn[1].message)
        assert 'Creating column ColumnC in table '+table_name in str(warn[2].message)
        results = sql.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_insert'].notna())


def test_insert_alter_column(sql):

    table_name = '##test_insert_alter_column'
    sql.create.table(table_name, columns={
        'ColumnA': 'TINYINT',
        'ColumnB': 'VARCHAR(1)',
        'ColumnC': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': ['aaa'], 'ColumnC': [100000]})

    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[1].message)
        assert 'Altering column ColumnC in table '+table_name in str(warn[2].message)
        results = sql.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_insert'].notna())

    schema = helpers.get_schema(sql.connection, table_name)
    columns,_,_,_ = helpers.flatten_schema(schema)
    assert columns=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'int', '_time_insert': 'datetime'}


def test_insert_add_and_alter_column(sql):

    table_name = '##test_insert_add_and_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [0,1,2,3],
        'ColumnB': [0,1,2,3]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index', row_count=1)

    dataframe['ColumnB'] = [256,257,258,259]
    dataframe['ColumnC'] = [0,1,2,3]
    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)        
        assert 'Creating column ColumnC in table '+table_name in str(warn[1].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[2].message)        
        results = sql.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_insert'].notna())


def test_update_create_table(sql):

    table_name = '##test_update_create_table'

    # create table to update
    dataframe = pd.DataFrame({
        '_pk': [0,1],
        'ColumnA': [1,2]
    }).set_index(keys='_pk')
    
    with pytest.raises(errors.SQLTableDoesNotExist):
        sql.write.update(table_name, dataframe)


def test_update_add_column(sql):
    
    table_name = '##test_update_add_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update using the SQL primary key that came from the dataframe's index
    dataframe['NewColumn'] = [3,4]
    with warnings.catch_warnings(record=True) as warn:
        sql.write.update(table_name, dataframe[['NewColumn']])
        assert len(warn)==2
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[1].message)
        result = sql.read.select(table_name)
        assert all(result[['ColumnA','NewColumn']]==dataframe)
        assert (result['_time_update'].notna()).all()


def test_update_alter_column(sql):

    table_name = '##test_update_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [0,0]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key=None)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update using ColumnA
    dataframe['ColumnB'] = ['aaa','bbb']
    dataframe['ColumnC'] = [256, 256]
    with warnings.catch_warnings(record=True) as warn:
        sql.write.update(table_name, dataframe, match_columns=['ColumnA'])
        assert len(warn)==5
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Altering column ColumnB in table ##update_'+table_name in str(warn[0].message)
        assert 'Altering column ColumnC in table ##update_'+table_name in str(warn[1].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[2].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[3].message)
        assert 'Altering column ColumnC in table '+table_name in str(warn[4].message)
        results = sql.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_update'].notna())

    schema = helpers.get_schema(sql.connection, table_name)
    columns,_,_,_ = helpers.flatten_schema(schema)
    assert columns=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'smallint', '_time_update': 'datetime'}


def test_update_add_and_alter_column(sql):

    table_name = '##test_update_add_and_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b']
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update using the SQL primary key that came from the dataframe's index
    dataframe['ColumnB'] = ['aaa','bbb']
    dataframe['NewColumn'] = [3,4]
    with warnings.catch_warnings(record=True) as warn:
        sql.write.update(table_name, dataframe[['ColumnB', 'NewColumn']])
        assert len(warn)==4
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Altering column ColumnB in table ##update_'+table_name in str(warn[1].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[2].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[3].message)
        result = sql.read.select(table_name)
        assert all(result[['ColumnA','ColumnB','NewColumn']]==dataframe)
        assert (result['_time_update'].notna()).all()


def test_merge_create_table(sql):

    table_name = "##test_merge_create_table"
    dataframe = pd.DataFrame({
            '_pk': [1,2],
            'ColumnA': [5,6]
        })

    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe, match_columns=['_pk'])
        assert len(warn)==2
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating table '+table_name in str(warn[0].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[1].message)
        results = sql.read.select(table_name)
        assert all(results[['_pk','ColumnA']]==dataframe)
        assert all(results['_time_insert'].notna())


def test_merge_add_column(sql):

    table_name = '##test_merge_add_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe['NewColumn'] = [3]
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[1].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[2].message)
        result = sql.read.select(table_name)
        assert all(result[['ColumnA','NewColumn']]==dataframe)
        assert all(result['_time_insert'].isna())
        assert all(result['_time_update'].notna())


def test_merge_alter_column(sql):

    table_name = '##test_merge_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b']
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[1,'ColumnA'] = 10000
    dataframe.loc[1,'ColumnB'] = 'bbbbb'
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe)
        assert len(warn)==6
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Altering column ColumnA in table ##merge_'+table_name in str(warn[0].message)
        assert 'Altering column ColumnB in table ##merge_'+table_name in str(warn[1].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[2].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[3].message)
        assert 'Altering column ColumnA in table '+table_name in str(warn[4].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[5].message)
        results = sql.read.select(table_name)
        assert all(results[['ColumnA','ColumnB']]==dataframe[['ColumnA','ColumnB']])
        assert all(results['_time_insert'].isna())
        assert all(results['_time_update'].notna())


def test_merge_add_and_alter_column(sql):

    table_name = '##test_merge_add_and_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b']
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[1,'ColumnA'] = 3
    dataframe.loc[1,'ColumnB'] = 'bbbbb'
    dataframe['NewColumn'] = 0
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe)
        assert len(warn)==5
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Altering column ColumnB in table ##merge_'+table_name in str(warn[1].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[2].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[3].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[4].message)        
        results = sql.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','NewColumn']]==dataframe[['ColumnA','ColumnB','NewColumn']])
        assert all(results['_time_insert'].isna())
        assert all(results['_time_update'].notna())