import pytest
import pandas as pd
import numpy as np
from datetime import date

from mssql_dataframe import connect
from mssql_dataframe import write
from mssql_dataframe import create
from mssql_dataframe import read
from mssql_dataframe import errors

@pytest.fixture(scope="module")
def connection():

    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


def test_prepare_values():

    dataframe = pd.DataFrame({
        'Column': [np.nan, pd.NA, None, pd.NaT]
    })
    dataframe = write.prepare_values(dataframe)
    assert all(dataframe['Column'].values==None)

    dataframe = pd.DataFrame({
        'Column': ['a  ','  b  ','c','','   '],
        
    })
    dataframe = write.prepare_values(dataframe)
    assert all(dataframe['Column'].values==['a','b','c',None,None])


def test_insert(connection):

    table_name = '##test_insert'
    create.table(connection, table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'INT',
            'ColumnC': 'BIGINT',
            'ColumnD': 'DATE',
            'ColumnE': 'VARCHAR(10)'
    })

    # single value
    dataframe = pd.DataFrame({'ColumnA': [1]})
    write.insert(connection, table_name, dataframe)

    # single column
    dataframe = pd.DataFrame({'ColumnB': [2,3,4]})
    write.insert(connection, table_name, dataframe)

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
    write.insert(connection, table_name, dataframe)

    # test all insertions
    results = read.select(connection, table_name)
    assert all(results.loc[results['ColumnA'].notna(),'ColumnA']==pd.Series([1,5,7], index=[0,4,6]))
    assert all(results.loc[results['ColumnB'].notna(),'ColumnB']==pd.Series([2,3,4,5,6], index=[1,2,3,4,5]))
    assert all(results.loc[results['ColumnC'].notna(),'ColumnC']==pd.Series([6,7], index=[5,6]))
    assert all(results.loc[results['ColumnD'].notna(),'ColumnD']==pd.Series([date(2021,6,22), date(2021,6,22)], index=[4,5]))
    assert all(results.loc[results['ColumnE'].notna(),'ColumnE']==pd.Series(['a','b'], index=[4,5]))


def test_update_no_table(connection):

    table_name = '##test_update_no_table'

    # create table to update
    dataframe = pd.DataFrame({
        '_pk': [0,1],
        'ColumnA': [1,2]
    }).set_index(keys='_pk')
    
    with pytest.raises(errors.TableDoesNotExist):
        # attempt updating table that does not exist
        write.update(connection, table_name, dataframe)


def test_update_one_match_column(connection):

    table_name = '##test_update_one_match_column'

    # create table to update
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    create.from_dataframe(connection, table_name, dataframe, primary_key='sql')

    # update values in table, using the primary key created in SQL
    dataframe['ColumnC'] = [5,6]
    pk = read.select(connection, table_name, column_names=['_pk','ColumnB'])
    dataframe = dataframe.merge(pk.reset_index()).set_index('_pk')
    write.update(connection, table_name, dataframe[['ColumnC']])

    # test result
    result = read.select(connection, table_name)
    expected = pd.DataFrame({'ColumnA': [1,2], 'ColumnB': ['a','b'], 'ColumnC': [5,6]})
    assert (expected.values==result[['ColumnA','ColumnB','ColumnC']].values).all()
    assert (result['_time_update'].notna()).all()


def test_update_two_match_columns(connection):

    table_name = '##test_update_two_match_columns'

    # create table to update
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    create.from_dataframe(connection, table_name, dataframe, primary_key='sql')

    # update values in table, using the primary key created in SQL and ColumnA
    dataframe = read.select(connection, table_name)
    dataframe['ColumnC'] = [5,6]
    write.update(connection, table_name, dataframe, match_columns=['_pk','ColumnA'])

    # test result
    result = read.select(connection, table_name)
    expected = pd.DataFrame({'ColumnA': [1,2], 'ColumnB': ['a','b'], 'ColumnC': [5,6]})
    assert (expected.values==result[['ColumnA','ColumnB','ColumnC']].values).all()
    assert (result['_time_update'].notna()).all()


def test_update_new_column(connection):
    
    table_name = '##test_update_new_column'

    # create table to update
    dataframe = pd.DataFrame({
        'ColumnA': [1,2]
    })
    create.from_dataframe(connection, table_name, dataframe, primary_key='index')

    # update values in table, using the primary key created in SQL
    dataframe['NewColumn'] = [3,4]
    write.update(connection, table_name, dataframe[['NewColumn']])

    # test result
    result = read.select(connection, table_name)
    expected = pd.DataFrame({'ColumnA': [1,2], 'NewColumn': [3,4]})
    assert (expected.values==result[['ColumnA','NewColumn']].values).all()
    assert (result['_time_update'].notna()).all()


def test_merge_no_table(connection):
    
    table_name = "##test_merge_no_table"
    
    with pytest.raises(errors.TableDoesNotExist):
        # attempt merge into table that doesn't exist
        dataframe = pd.DataFrame({
            '_pk': [1,2],
            'ColumnA': [5,6]
        })
        write.merge(connection, table_name, dataframe, match_columns=['_pk'])


def test_merge_one_match_column(connection):
    
    table_name = "##test_merge_one_match_column"

    # create table to merge into
    dataframe = pd.DataFrame({
        'ColumnA': [3,4]
    })
    create.from_dataframe(connection, table_name, dataframe, primary_key='index')

    # perform merge
    dataframe = pd.DataFrame({
        '_index': [1,2],
        'ColumnA': [5,6]
    })
    write.merge(connection, table_name, dataframe)

    # test result
    result = read.select(connection, table_name)
    expected = pd.DataFrame({'ColumnA': [5,6]}, index=[1,2])
    expected.index.name='_index'
    assert (expected.values==result[['ColumnA']].values).all()
    assert all(result.loc[1,['_time_update']].notna())
    assert all(result.loc[1,['_time_insert']].isna())
    assert all(result.loc[2,['_time_insert']].notna())
    assert all(result.loc[2,['_time_update']].isna())


def test_merge_two_match_columns(connection):
    table_name = "##test_merge_two_match_columns"

    # create table to merge into
    dataframe = pd.DataFrame({
        '_pk': [0,1],
        'State': ['A','B'],
        'ColumnA': [3,4],
        'ColumnB': ['a','b']
    })
    create.from_dataframe(connection, table_name, dataframe, primary_key=None)

    # perform merge
    dataframe = pd.DataFrame({
        '_pk': [1,2],
        'State': ['B','C'],
        'ColumnA': [5,6],
        'ColumnB': ['c','d']
    })
    write.merge(connection, table_name, dataframe, match_columns=['_pk','State'])

    # test result
    result = read.select(connection, table_name)
    expected = pd.DataFrame({'_pk': [1,2], 'State':['B','C'], 'ColumnA': [5,6], 'ColumnB': ['c','d']})
    assert (expected.values==result[['_pk','State','ColumnA','ColumnB']].values).all()
    assert all(result.loc[result['_pk']==1,'_time_update'].notna())
    assert all(result.loc[result['_pk']==1,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==2,'_time_insert'].notna())
    assert all(result.loc[result['_pk']==2,'_time_update'].isna())


def test_merge_one_subset_column(connection):
    
    table_name = "##test_merge_one_subset_column"

    # create table to merge into
    dataframe = pd.DataFrame({
        '_pk': [0,1,2],
        'State': ['A','B','B'],
        'ColumnA': [3,4,4],
        'ColumnB': ['a','b','b']
    })
    create.from_dataframe(connection, table_name, dataframe, primary_key=None)

    # perform merge
    dataframe = pd.DataFrame({
        '_pk': [1,3],
        'State': ['B','C'],
        'ColumnA': [5,6],
        'ColumnB': ['c','d']
    })
    write.merge(connection, table_name, dataframe, match_columns=['_pk'], subset_columns=['State'])

    # test result
    result = read.select(connection, table_name)
    expected = pd.DataFrame({'_pk': [0,1,3], 'State':['A','B','C'], 'ColumnA': [3,5,6], 'ColumnB': ['a','c','d']})
    assert (expected.values==result[['_pk','State','ColumnA','ColumnB']].values).all()
    assert all(result.loc[result['_pk']==0,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==0,'_time_update'].isna())
    assert all(result.loc[result['_pk']==1,'_time_update'].notna())
    assert all(result.loc[result['_pk']==1,'_time_insert'].isna())
    assert all(result.loc[result['_pk']==3,'_time_insert'].notna())
    assert all(result.loc[result['_pk']==3,'_time_update'].isna())


def test_merge_two_subset_columns(connection):

    table_name = "##test_merge_two_subset_columns"

    # create table to merge into
    dataframe = pd.DataFrame({
        '_pk': [0,1,2],
        'State1': ['A','B','B'],
        'State2': ['X','Y','Z'],
        'ColumnA': [3,4,4],
        'ColumnB': ['a','b','b']
    })
    create.from_dataframe(connection, table_name, dataframe, primary_key=None)

    # perform merge
    dataframe = pd.DataFrame({
        '_pk': [1,3],
        'State1': ['B','C'],
        'State2': ['Y','Z'],
        'ColumnA': [5,6],
        'ColumnB': ['c','d']
    })
    write.merge(connection, table_name, dataframe, match_columns=['_pk'], subset_columns=['State1','State2'])

    # test result
    result = read.select(connection, table_name)
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