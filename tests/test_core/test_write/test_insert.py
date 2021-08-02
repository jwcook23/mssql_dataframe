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


def test_insert_errors(sql, sql_adjustable):

    table_name = '##test_insert_errors'
    sql.create.table(table_name, columns={
            'ColumnA': 'SMALLINT',
            'ColumnB': 'VARCHAR(1)',
            'ColumnD': 'DATETIME'
    })

    with pytest.raises(errors.SQLTableDoesNotExist):
        sql.write.insert('error'+table_name, dataframe=pd.DataFrame({'ColumnA': [1]}), include_timestamps=False)

    with pytest.raises(errors.SQLColumnDoesNotExist):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnC': [1]}), include_timestamps=False)

    with pytest.raises(errors.SQLInsufficientColumnSize):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnB': ['aaa']}), include_timestamps=False)

    with pytest.raises(errors.SQLInsufficientColumnSize):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnA': [100000]}), include_timestamps=False)

    # date value that can be converted
    sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnD': ['06/22/2021']}), include_timestamps=False)

    # string that cannot be converted to their Python equalivant based on SQL data type
    with pytest.raises(errors.SQLInvalidDataType):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnA': ['abs']}), include_timestamps=False)
    with pytest.raises(errors.SQLInvalidDataType):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnA': ['12-5']}), include_timestamps=False)
    with pytest.raises(errors.SQLInvalidDataType):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnA': ['12345-67589']}), include_timestamps=False)
    with pytest.raises(errors.SQLInvalidDataType):
        sql_adjustable.write.insert(table_name, dataframe=pd.DataFrame({'ColumnA': ['12345-67589']}), include_timestamps=False)


def test_insert(sql):

    table_name = '##test_insert'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'INT',
            'ColumnC': 'BIGINT',
            'ColumnD': 'DATE',
            'ColumnE': 'VARCHAR(10)'
    })

    # single value
    dataframe = pd.DataFrame({'ColumnA': [1]})
    with warnings.catch_warnings(record=True) as warn:
        sql.write.insert(table_name, dataframe)
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_insert' in str(warn[0].message)

    # single column
    dataframe = pd.DataFrame({'ColumnB': [2,3,4]})
    sql.write.insert(table_name, dataframe)

    # entire dataframe
    dataframe = pd.DataFrame({
        'ColumnA': [5,np.nan,7],
        'ColumnB': [5,6,None],
        'ColumnC': [pd.NA,6,7],
        'ColumnD': ['06-22-2021','06-22-2021',pd.NaT],
        'ColumnE' : ['a','b',None]
    })
    dataframe['ColumnB'] = dataframe['ColumnB'].astype('Int64')
    dataframe['ColumnD'] = pd.to_datetime(dataframe['ColumnD'])
    sql.write.insert(table_name, dataframe)

    # single column of dates
    dataframe = pd.DataFrame({'ColumnD': ['06-22-2021','06-22-2021']}, dtype='datetime64[ns]')
    sql.write.insert(table_name, dataframe)

    # test all insertions
    results = sql.read.select(table_name)
    assert all(results.loc[results['ColumnA'].notna(),'ColumnA']==pd.Series([1,5,7], index=[0,4,6]))
    assert all(results.loc[results['ColumnB'].notna(),'ColumnB']==pd.Series([2,3,4,5,6], index=[1,2,3,4,5]))
    assert all(results.loc[results['ColumnC'].notna(),'ColumnC']==pd.Series([6,7], index=[5,6]))
    assert all(results.loc[results['ColumnD'].notna(),'ColumnD']==pd.Series([date(2021,6,22)]*4, index=[4,5,7,8]))
    assert all(results.loc[results['ColumnE'].notna(),'ColumnE']==pd.Series(['a','b'], index=[4,5]))
    assert all(results['_time_insert'].notna())


def test_insert_composite_pk(sql):
    
    table_name = '##test_insert_composite_pk'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'VARCHAR(5)',
            'ColumnC': 'BIGINT'
    }, primary_key_column=['ColumnA','ColumnB'])

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [12345]})
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    results = sql.read.select(table_name)
    assert len(results)==1
    assert pd.isnull(results.at[(1,'12345'),'ColumnC'])


def test_insert_exclude_timestamps(sql):

    table_name = '##test_insert_exclude_timestamps'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT'
    })
    dataframe = pd.DataFrame({"ColumnA": [1,2,3]})
    sql.write.insert(table_name, dataframe, include_timestamps=False)
    results = sql.read.select(table_name)
    assert all(results==dataframe)


def test_insert_create_table(sql_adjustable):

    table_name = '##test_insert_create_table'

    # SQL will infer '06/22/2021' as a date but not allow for insertion
    # # insure internally value is first created to datetime which will work
    dataframe = pd.DataFrame({
        "ColumnA": [1,2,3],
        "ColumnB": ['06/22/2021','06-22-2021','2023-08-31']
    })

    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating table '+table_name in str(warn[0].message)
        assert 'Created table '+table_name in str(warn[1].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[2].message)        
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA']]==dataframe[['ColumnA']])
        assert all(results['_time_insert'].notna())


def test_insert_add_column(sql_adjustable):

    table_name = '##test_insert_add_column'
    sql_adjustable.create.table(table_name, columns={
        'ColumnA': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [2], 'ColumnC': [3]})

    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)        
        assert 'Creating column ColumnB in table '+table_name in str(warn[1].message)
        assert 'Creating column ColumnC in table '+table_name in str(warn[2].message)
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_insert'].notna())


def test_insert_alter_column(sql_adjustable):

    table_name = '##test_insert_alter_column'
    sql_adjustable.create.table(table_name, columns={
        'ColumnA': 'TINYINT',
        'ColumnB': 'VARCHAR(1)',
        'ColumnC': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': ['aaa'], 'ColumnC': [100000]})

    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.insert(table_name, dataframe=dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[1].message)
        assert 'Altering column ColumnC in table '+table_name in str(warn[2].message)
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_insert'].notna())

    schema = helpers.get_schema(sql_adjustable.connection, table_name)
    columns,_,_,_ = helpers.flatten_schema(schema)
    assert columns=={'ColumnA': 'tinyint', 'ColumnB': 'varchar(3)', 'ColumnC': 'int', '_time_insert': 'datetime'}


def test_insert_alter_primary_key(sql_adjustable):

    table_name = '##test_insert_alter_primary_key'
    dataframe = pd.DataFrame({
        'ColumnA': [0,1,2,3],
        'ColumnB': [0,1,2,3]
    }).set_index(keys='ColumnA')
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)

    sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

    new = pd.DataFrame({
        'ColumnA': [256,257,258,259],
        'ColumnB': [4,5,6,7]
    }).set_index(keys='ColumnA')
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.insert(table_name, new, include_timestamps=False)
        assert len(warn)==1
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Altering column ColumnA in table '+table_name in str(warn[0].message)        
        results = sql_adjustable.read.select(table_name)
        assert all(results==dataframe.append(new))


def test_insert_add_and_alter_column(sql_adjustable):

    table_name = '##test_insert_add_and_alter_column'
    dataframe = pd.DataFrame({
        'ColumnA': [0,1,2,3],
        'ColumnB': [0,1,2,3]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index', row_count=1)
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)

    dataframe['ColumnB'] = [256,257,258,259]
    dataframe['ColumnC'] = [0,1,2,3]
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.insert(table_name, dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)        
        assert 'Creating column ColumnC in table '+table_name in str(warn[1].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[2].message)        
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
        assert all(results['_time_insert'].notna())