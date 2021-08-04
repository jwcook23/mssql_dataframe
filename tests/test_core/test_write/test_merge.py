from datetime import date
import warnings

import pytest
import pandas as pd
import numpy as np
import pyodbc

from mssql_dataframe import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors


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
        

def test_merge_errors(sql):
    
    table_name = "##test_merge_errors"
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'VARCHAR(1)'
    })
 
    with pytest.raises(errors.SQLTableDoesNotExist):
        sql.write.merge('error'+table_name, dataframe=pd.DataFrame({'ColumnA': [1]}))

    with pytest.raises(errors.SQLColumnDoesNotExist):
        sql.write.merge(table_name, dataframe=pd.DataFrame({'ColumnA': [0],'ColumnC': [1]}), match_columns=['ColumnA'])

    with pytest.raises(errors.SQLInsufficientColumnSize):
        sql.write.merge(table_name, dataframe=pd.DataFrame({'ColumnA': [100000],'ColumnB': ['aaa']}), match_columns=['ColumnA'])

    with pytest.raises(ValueError):
        sql.write.merge(table_name, dataframe=pd.DataFrame({'ColumnA': [100000],'ColumnB': ['aaa']}), delete_unmatched=False, delete_conditions=["ColumnB"])


def test_merge_keep_unmatched(sql):
    
    table_name = "##test_merge keep_unmatched"
    dataframe = pd.DataFrame({
        'ColumnA': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge values into table, using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[dataframe.index==1,'ColumnA'] = 5
    dataframe = dataframe.append(pd.Series([6], index=['ColumnA'], name=2))
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe, delete_unmatched=False)
        assert len(warn)==2
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_insert' in str(warn[0].message)
        assert isinstance(warn[1].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_update' in str(warn[1].message)
    result = sql.read.select(table_name)
    assert all(result[['ColumnA']]==pd.DataFrame({'ColumnA': [3,5,6]}, index=[0,1,2]))
    assert all(result.loc[0,['_time_insert']].isna())
    assert all(result.loc[0,['_time_update']].isna())
    assert all(result.loc[1,['_time_insert']].isna())
    assert all(result.loc[1,['_time_update']].notna())
    assert all(result.loc[2,['_time_insert']].notna())
    assert all(result.loc[2,['_time_update']].isna())


def test_merge_one_match_column(sql):
    
    table_name = "##test_merge_one_match_column"
    dataframe = pd.DataFrame({
        'ColumnA': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge values into table, using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[dataframe.index==1,'ColumnA'] = 5
    dataframe = dataframe.append(pd.Series([6], index=['ColumnA'], name=2))
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe)
        assert len(warn)==2
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_insert' in str(warn[0].message)
        assert isinstance(warn[1].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_update' in str(warn[1].message)
    result = sql.read.select(table_name)
    assert all(result[['ColumnA']]==dataframe[['ColumnA']])
    assert all(result.loc[1,['_time_update']].notna())
    assert all(result.loc[1,['_time_insert']].isna())
    assert all(result.loc[2,['_time_insert']].notna())
    assert all(result.loc[2,['_time_update']].isna())


def test_merge_two_match_columns(sql):

    table_name = "##test_merge_two_match_columns"
    dataframe = pd.DataFrame({
        'State': ['A','B'],
        'ColumnA': [3,4],
        'ColumnB': ['a','b']
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge values into table, using the primary key that came from the dataframe's index and ColumnA
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[dataframe.index==1,'ColumnA'] = 5
    dataframe = dataframe.append(pd.DataFrame({'State': ['C'], 'ColumnA': [6], 'ColumnB': ['d']}, index=[2]))
    dataframe.index.name = '_index'
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe, match_columns=['_index','State'])
        assert len(warn)==2
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_insert' in str(warn[0].message)
        assert isinstance(warn[1].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_update' in str(warn[1].message)
    result = sql.read.select(table_name)
    assert all(result[['State','ColumnA','ColumnB']]==dataframe[['State','ColumnA','ColumnB']])
    assert all(result.loc[result.index==1,'_time_update'].notna())
    assert all(result.loc[result.index==1,'_time_insert'].isna())
    assert all(result.loc[result.index==2,'_time_insert'].notna())
    assert all(result.loc[result.index==2,'_time_update'].isna())


def test_merge_composite_pk(sql):

    table_name = "##test_merge_composite_pk"
    dataframe = pd.DataFrame({
        'State': ['A','B'],
        'ColumnA': [3,4],
        'ColumnB': ['a','b']
    })
    dataframe = dataframe.set_index(keys=['State','ColumnA'])
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    dataframe = dataframe[dataframe.index!=('A',3)]
    dataframe.loc[dataframe.index==('B',4),'ColumnB'] = 'c'
    dataframe = dataframe.append(
        pd.DataFrame({'State': ['C'], 'ColumnA': [6], 'ColumnB': ['d']}).set_index(keys=['State','ColumnA'])
    )
    sql.write.merge(table_name, dataframe, include_timestamps=False)

    result = sql.read.select(table_name)
    assert all(result[['ColumnB']]==dataframe[['ColumnB']])
    

def test_merge_one_delete_condition(sql):
    
    table_name = "##test_merge_one_delete_condition"
    dataframe = pd.DataFrame({
        'State': ['A','B','B'],
        'ColumnA': [3,4,4],
        'ColumnB': ['a','b','b']
    }, index=[0,1,2])
    dataframe.index.name='_pk'
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge values into table, using the primary key that came from the dataframe's index
    # also require a match on State to prevent a record from being deleted
    dataframe = dataframe[dataframe.index==1]
    dataframe.loc[dataframe.index==1,'ColumnA'] = 5
    dataframe.loc[dataframe.index==1,'ColumnB'] = 'c'
    dataframe = dataframe.append(pd.DataFrame({'State': ['C'], 'ColumnA': [6], 'ColumnB': ['d']}, index=[3]))
    dataframe.index.name = '_pk'
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe, match_columns=['_pk'], delete_conditions=['State'])
        assert len(warn)==2
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_insert' in str(warn[0].message)
        assert isinstance(warn[1].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_update' in str(warn[1].message)    
    result = sql.read.select(table_name)
    assert all(result.loc[[1,3],['State','ColumnA','ColumnB']]==dataframe)
    assert all(result.loc[0,['State','ColumnA','ColumnB']]==pd.Series(['A',3,'a'], index=['State','ColumnA','ColumnB']))
    assert all(result.loc[result.index==0,'_time_insert'].isna())
    assert all(result.loc[result.index==0,'_time_update'].isna())
    assert all(result.loc[result.index==1,'_time_update'].notna())
    assert all(result.loc[result.index==1,'_time_insert'].isna())
    assert all(result.loc[result.index==3,'_time_insert'].notna())
    assert all(result.loc[result.index==3,'_time_update'].isna())


def test_merge_two_delete_conditions(sql):

    table_name = "##test_merge_two_delete_conditions"
    dataframe = pd.DataFrame({
        'State1': ['A','B','B'],
        'State2': ['X','Y','Z'],
        'ColumnA': [3,4,4],
        'ColumnB': ['a','b','b']
    }, index=[0,1,2])
    dataframe.index.name = '_pk'
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge values into table, using the primary key that came from the dataframe's index
    # also require a match on State1 and State2 to prevent a record from being deleted
    dataframe = dataframe[dataframe.index==1]
    dataframe.loc[dataframe.index==1,'ColumnA'] = 5
    dataframe.loc[dataframe.index==1,'ColumnB'] = 'c'
    dataframe = dataframe.append(pd.DataFrame({'State1': ['C'], 'State2': ['Z'], 'ColumnA': [6], 'ColumnB': ['d']}, index=[3]))
    dataframe.index.name = '_pk'
    with warnings.catch_warnings(record=True) as warn:
        sql.write.merge(table_name, dataframe, match_columns=['_pk'], delete_conditions=['State1','State2'])
        assert len(warn)==2
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_insert' in str(warn[0].message)
        assert isinstance(warn[1].message, errors.SQLObjectAdjustment)
        assert 'Creating column _time_update' in str(warn[1].message)   
    result = sql.read.select(table_name)
    assert all(result.loc[[1,3],['State1','State2','ColumnA','ColumnB']]==dataframe)
    assert all(result.loc[0,['State1','State2','ColumnA','ColumnB']]==pd.Series(['A','X',3,'a'], index=['State1','State2','ColumnA','ColumnB']))
    assert all(result.loc[result.index==0,'_time_insert'].isna())
    assert all(result.loc[result.index==0,'_time_update'].isna())
    assert all(result.loc[result.index==1,'_time_update'].notna())
    assert all(result.loc[result.index==1,'_time_insert'].isna())
    assert all(result.loc[result.index==3,'_time_insert'].notna())
    assert all(result.loc[result.index==3,'_time_update'].isna())


def test_merge_exclude_timestamps(sql):
    
    table_name = "##test_merge_exclude_timestamps"
    dataframe = pd.DataFrame({
        'ColumnA': [3,4]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # merge values into table, using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[dataframe.index==1,'ColumnA'] = 5
    dataframe = dataframe.append(pd.Series([6], index=['ColumnA'], name=2))
    sql.write.merge(table_name, dataframe, include_timestamps=False)
    result = sql.read.select(table_name)
    assert all(result[['ColumnA']]==dataframe[['ColumnA']])
    assert '_time_insert' not in result.columns
    assert '_time_update' not in result.columns


def test_merge_create_table(sql_adjustable):

    table_name = "##test_merge_create_table"
    dataframe = pd.DataFrame({
            '_pk': [1,2],
            'ColumnA': [5,6],
            'ColumnB': ['06/22/2021','2023-08-31']
        })

    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.merge(table_name, dataframe, match_columns=['_pk'])
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating table '+table_name in str(warn[0].message)
        assert 'Created table '+table_name in str(warn[1].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[2].message)
        results = sql_adjustable.read.select(table_name)
        assert all(results[['_pk','ColumnA','ColumnB']]==dataframe)
        assert all(results['_time_insert'].notna())
        

def test_merge_add_column(sql_adjustable):

    table_name = '##test_merge_add_column'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2]
    })
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.create.table_from_dataframe(table_name, dataframe, primary_key='index')
        assert len(warn)==1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert 'Created table' in str(warn[0].message)
    sql_adjustable.write.insert(table_name, dataframe, include_timestamps=False)

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe['NewColumn'] = [3]
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.merge(table_name, dataframe)
        assert len(warn)==3
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[1].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[2].message)
        result = sql_adjustable.read.select(table_name)
        assert all(result[['ColumnA','NewColumn']]==dataframe)
        assert all(result['_time_insert'].isna())
        assert all(result['_time_update'].notna())


def test_merge_alter_column(sql_adjustable):

    table_name = '##test_merge_alter_column'
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

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[1,'ColumnA'] = 10000
    dataframe.loc[1,'ColumnB'] = 'bbbbb'
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.merge(table_name, dataframe)
        assert len(warn)==4
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column _time_insert in table '+table_name in str(warn[0].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[1].message)
        assert 'Altering column ColumnA in table '+table_name in str(warn[2].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[3].message)
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA','ColumnB']]==dataframe[['ColumnA','ColumnB']])
        assert all(results['_time_insert'].isna())
        assert all(results['_time_update'].notna())


def test_merge_add_and_alter_column(sql_adjustable):

    table_name = '##test_merge_add_and_alter_column'
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

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index!=0]
    dataframe.loc[1,'ColumnA'] = 3
    dataframe.loc[1,'ColumnB'] = 'bbbbb'
    dataframe['NewColumn'] = 0
    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable.write.merge(table_name, dataframe)
        assert len(warn)==4
        assert all([isinstance(x.message, errors.SQLObjectAdjustment) for x in warn])
        assert 'Creating column NewColumn in table '+table_name in str(warn[0].message)
        assert 'Creating column _time_insert in table '+table_name in str(warn[1].message)
        assert 'Creating column _time_update in table '+table_name in str(warn[2].message)
        assert 'Altering column ColumnB in table '+table_name in str(warn[3].message)        
        results = sql_adjustable.read.select(table_name)
        assert all(results[['ColumnA','ColumnB','NewColumn']]==dataframe[['ColumnA','ColumnB','NewColumn']])
        assert all(results['_time_insert'].isna())
        assert all(results['_time_update'].notna())