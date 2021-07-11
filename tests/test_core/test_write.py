from datetime import date
import warnings

import pytest
import pandas as pd
import numpy as np

from mssql_dataframe import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors #, create, write, read


# class package:
#     def __init__(self, connection):
#         self.create = create.create(connection)
#         self.write = write.write(connection)
#         self.read = read.read(connection)

@pytest.fixture(scope="module")
def sql():
    connection = connect.connect(database_name='tempdb', server_name='localhost', autocommit=False)
    yield SQLServer(connection, adjust_sql_objects=False)
    connection.connection.close()

@pytest.fixture(scope="module")
def sql_adjustable():
    connection = connect.connect(database_name='tempdb', server_name='localhost', autocommit=False)
    yield SQLServer(connection, adjust_sql_objects=True)
    connection.connection.close()


def test_prepare_values(sql):

    dataframe = pd.DataFrame({
        'Column': [np.nan, pd.NA, None, pd.NaT]
    })
    dataframe = sql.write._write__prepare_values(dataframe)
    assert all(dataframe['Column'].values==None)

    dataframe = pd.DataFrame({
        'Column': ['a  ','  b  ','c','','   '],
        
    })
    dataframe = sql.write._write__prepare_values(dataframe)
    assert all(dataframe['Column'].values==['a','b','c',None,None])


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
    sql.write.insert(table_name, dataframe)

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

    # test all insertions
    # get = mssql_dataframe.read.read(connection)
    results = sql.read.select(table_name)
    assert all(results.loc[results['ColumnA'].notna(),'ColumnA']==pd.Series([1,5,7], index=[0,4,6]))
    assert all(results.loc[results['ColumnB'].notna(),'ColumnB']==pd.Series([2,3,4,5,6], index=[1,2,3,4,5]))
    assert all(results.loc[results['ColumnC'].notna(),'ColumnC']==pd.Series([6,7], index=[5,6]))
    assert all(results.loc[results['ColumnD'].notna(),'ColumnD']==pd.Series([date(2021,6,22), date(2021,6,22)], index=[4,5]))
    assert all(results.loc[results['ColumnE'].notna(),'ColumnE']==pd.Series(['a','b'], index=[4,5]))


def test_insert_errors(sql):

    table_name = '##test_insert_errors'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'VARCHAR(1)'
    })

    with pytest.raises(errors.SQLTableDoesNotExist):
        sql.write.insert('error'+table_name, dataframe=pd.DataFrame({'ColumnA': [1]}))

    with pytest.raises(errors.SQLColumnDoesNotExist):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnC': [1]}))

    with pytest.raises(errors.SQLInsufficientStringColumnSize):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnB': ['aaa']}))

    with pytest.raises(errors.SQLInsufficientNumericColumnSize):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnA': [100000]}))


def test_insert_create_table(sql_adjustable):

    table_name = '##test_insert_create_table'
    dataframe = pd.DataFrame({
        "ColumnA": [1,2]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.insert(table_name, dataframe=dataframe)
        results = sql_adjustable.read.select(table_name)
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert all(results==dataframe)


def test_insert_add_column(sql_adjustable):

    table_name = '##test_insert_add_column'
    sql_adjustable.create.table(table_name, columns={
            'ColumnA': 'TINYINT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1], 'ColumnB': [2]})

    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.insert(table_name, dataframe=dataframe)
        results = sql_adjustable.read.select(table_name)
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert all(results==dataframe)


def test_insert_alter_column(sql):
    pass


def test__prep_update_merge(sql):

    table_name = '##test__prep_update_merge'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'INT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1]})

    with pytest.raises(errors.SQLUndefinedPrimaryKey):
        sql.write._write__prep_update_merge(table_name, match_columns=None, dataframe=dataframe, operation='update')

    with pytest.raises(errors.SQLUndefinedColumn):
        sql.write._write__prep_update_merge(table_name, match_columns='MissingColumn', dataframe=dataframe, operation='update')       

    with pytest.raises(errors.DataframeUndefinedColumn):
        sql.write._write__prep_update_merge(table_name, match_columns='ColumnB', dataframe=dataframe, operation='update')   


def test_update_no_table(sql):

    table_name = '##test_update_no_table'

    # create table to update
    dataframe = pd.DataFrame({
        '_pk': [0,1],
        'ColumnA': [1,2]
    }).set_index(keys='_pk')
    
    with pytest.raises(errors.SQLTableDoesNotExist):
        # attempt updating table that does not exist
        sql.write.update(table_name, dataframe)


def test_update_one_match_column(sql):

    table_name = '##test_update_one_match_column'

    # create table to update
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe)

    # update values in table, using the primary key
    dataframe['ColumnC'] = [5,6]
    sql.write.update(table_name, dataframe[['ColumnC']])

    # test result
    result = sql.read.select(table_name)
    expected = pd.DataFrame({'ColumnA': [1,2], 'ColumnB': ['a','b'], 'ColumnC': [5,6]})
    assert (expected.values==result[['ColumnA','ColumnB','ColumnC']].values).all()
    assert (result['_time_update'].notna()).all()


def test_update_two_match_columns(sql):

    table_name = '##test_update_two_match_columns'

    # create table to update
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='sql')
    sql.write.insert(table_name, dataframe)

    # update values in table, using the primary key created in SQL and ColumnA
    dataframe = sql.read.select(table_name)
    dataframe['ColumnC'] = [5,6]
    sql.write.update(table_name, dataframe, match_columns=['_pk','ColumnA'])

    # test result
    result = sql.read.select(table_name)
    expected = pd.DataFrame({'ColumnA': [1,2], 'ColumnB': ['a','b'], 'ColumnC': [5,6]})
    assert (expected.values==result[['ColumnA','ColumnB','ColumnC']].values).all()
    assert (result['_time_update'].notna()).all()


def test_update_new_column(sql):
    
    table_name = '##test_update_new_column'

    # create table to update
    dataframe = pd.DataFrame({
        'ColumnA': [1,2]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe)

    # update values in table, using the primary key created in SQL
    dataframe['NewColumn'] = [3,4]
    sql.write.update(table_name, dataframe[['NewColumn']])

    # test result
    result = sql.read.select(table_name)
    expected = pd.DataFrame({'ColumnA': [1,2], 'NewColumn': [3,4]})
    assert (expected.values==result[['ColumnA','NewColumn']].values).all()
    assert (result['_time_update'].notna()).all()


def test_merge_no_table(sql):
    
    table_name = "##test_merge_no_table"
    
    with pytest.raises(errors.SQLTableDoesNotExist):
        # attempt merge into table that doesn't exist
        dataframe = pd.DataFrame({
            '_pk': [1,2],
            'ColumnA': [5,6]
        })
        sql.write.merge(table_name, dataframe, match_columns=['_pk'])


def test_merge_one_match_column(sql):
    
    table_name = "##test_merge_one_match_column"

    # create table to merge into
    dataframe = pd.DataFrame({
        'ColumnA': [3,4]
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe)

    # perform merge
    dataframe = pd.DataFrame({
        '_index': [1,2],
        'ColumnA': [5,6]
    })
    sql.write.merge(table_name, dataframe)

    # test result
    result = sql.read.select(table_name)
    expected = pd.DataFrame({'ColumnA': [5,6]}, index=[1,2])
    expected.index.name='_index'
    assert (expected.values==result[['ColumnA']].values).all()
    assert all(result.loc[1,['_time_update']].notna())
    assert all(result.loc[1,['_time_insert']].isna())
    assert all(result.loc[2,['_time_insert']].notna())
    assert all(result.loc[2,['_time_update']].isna())


def test_merge_two_match_columns(sql):
    table_name = "##test_merge_two_match_columns"

    # create table to merge into
    dataframe = pd.DataFrame({
        '_pk': [0,1],
        'State': ['A','B'],
        'ColumnA': [3,4],
        'ColumnB': ['a','b']
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key=None)
    sql.write.insert(table_name, dataframe)

    # perform merge
    dataframe = pd.DataFrame({
        '_pk': [1,2],
        'State': ['B','C'],
        'ColumnA': [5,6],
        'ColumnB': ['c','d']
    })
    sql.write.merge(table_name, dataframe, match_columns=['_pk','State'])

    # test result
    result = sql.read.select(table_name)
    expected = pd.DataFrame({'_pk': [1,2], 'State':['B','C'], 'ColumnA': [5,6], 'ColumnB': ['c','d']})
    assert (expected.values==result[['_pk','State','ColumnA','ColumnB']].values).all()
    assert all(result.loc[result['_pk']==1,'_time_update'].notna())
    assert all(result.loc[result['_pk']==1,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==2,'_time_insert'].notna())
    assert all(result.loc[result['_pk']==2,'_time_update'].isna())


def test_merge_one_subset_column(sql):
    
    table_name = "##test_merge_one_subset_column"

    # create table to merge into
    dataframe = pd.DataFrame({
        '_pk': [0,1,2],
        'State': ['A','B','B'],
        'ColumnA': [3,4,4],
        'ColumnB': ['a','b','b']
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key=None)
    sql.write.insert(table_name, dataframe)

    # perform merge
    dataframe = pd.DataFrame({
        '_pk': [1,3],
        'State': ['B','C'],
        'ColumnA': [5,6],
        'ColumnB': ['c','d']
    })
    sql.write.merge(table_name, dataframe, match_columns=['_pk'], subset_columns=['State'])

    # test result
    result = sql.read.select(table_name)
    expected = pd.DataFrame({'_pk': [0,1,3], 'State':['A','B','C'], 'ColumnA': [3,5,6], 'ColumnB': ['a','c','d']})
    assert (expected.values==result[['_pk','State','ColumnA','ColumnB']].values).all()
    assert all(result.loc[result['_pk']==0,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==0,'_time_update'].isna())
    assert all(result.loc[result['_pk']==1,'_time_update'].notna())
    assert all(result.loc[result['_pk']==1,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==3,'_time_insert'].notna())
    assert all(result.loc[result['_pk']==3,'_time_update'].isna())


def test_merge_two_subset_columns(sql):

    table_name = "##test_merge_two_subset_columns"

    # create table to merge into
    dataframe = pd.DataFrame({
        '_pk': [0,1,2],
        'State1': ['A','B','B'],
        'State2': ['X','Y','Z'],
        'ColumnA': [3,4,4],
        'ColumnB': ['a','b','b']
    })
    dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key=None)
    sql.write.insert(table_name, dataframe)

    # perform merge
    dataframe = pd.DataFrame({
        '_pk': [1,3],
        'State1': ['B','C'],
        'State2': ['Y','Z'],
        'ColumnA': [5,6],
        'ColumnB': ['c','d']
    })
    sql.write.merge(table_name, dataframe, match_columns=['_pk'], subset_columns=['State1','State2'])

    # test result
    result = sql.read.select(table_name)
    expected = pd.DataFrame({
        '_pk': [0,1,3], 'State1':['A','B','C'], 'State2':['X','Y','Z'], 'ColumnA': [3,5,6], 'ColumnB': ['a','c','d']
    })
    assert (expected.values==result[['_pk','State1','State2','ColumnA','ColumnB']].values).all()
    assert all(result.loc[result['_pk']==0,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==0,'_time_update'].isna())
    assert all(result.loc[result['_pk']==1,'_time_update'].notna())
    assert all(result.loc[result['_pk']==1,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==3,'_time_insert'].notna())
    assert all(result.loc[result['_pk']==3,'_time_update'].isna())