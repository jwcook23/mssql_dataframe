from datetime import date
import warnings

import pytest
import pandas as pd
import numpy as np

from mssql_dataframe import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors


@pytest.fixture(scope="module")
def sql():
    connection = connect.connect(database_name='tempdb', server_name='localhost', autocommit=False)
    yield SQLServer(connection, adjust_sql_objects=False)
    connection.connection.close()


def test_attempt_write_failure(sql):

    table_name = '##test_attempt_write_failure'
    sql.create.table(table_name, columns={'ColumnA': 'TINYINT'}, primary_key_column='ColumnA')

    dataframe = pd.DataFrame({'ColumnA': [1,1]})
    cursor_method = sql.connection.cursor.executemany
    statement = f'INSERT INTO {table_name} VALUES(?)'
    args = dataframe.values.tolist()

    # pyodb IntegrityError raised from duplicate primary key value
    # mask the error since it is unhandled
    with pytest.raises(errors.SQLGeneral):
        sql.write._write__attempt_write(table_name, dataframe, cursor_method, statement, args)


def test_prepare_values(sql):

    dataframe = pd.DataFrame({
        'Column': [np.nan, pd.NA, None, pd.NaT]
    })
    dataframe = sql.write._write__prepare_values(dataframe)
    assert all(dataframe['Column'].values==None)

    dataframe = pd.DataFrame({
        'ColumnA': ['a  ','  b  ','c','','   '],
        'ColumnB': [pd.Timedelta("0 days 01:00:00.123456789")]*5,
        'ColumnC': [pd.Timedelta("0 days 00:00:00.1234")]*5
    })

    dataframe = sql.write._write__prepare_values(dataframe)
    assert all(dataframe['ColumnA'].values==['a','b','c',None,None])
    assert all(dataframe['ColumnB'].values==['01:00:00.1234567']*5)
    assert all(dataframe['ColumnC'].values==['00:00:00.123400']*5)

    with pytest.raises(ValueError):
        sql.write._write__prepare_values(pd.DataFrame({'Column': [pd.Timedelta(days=1)]}))

    with pytest.raises(ValueError):
        sql.write._write__prepare_values(pd.DataFrame({'Column': [pd.Timedelta(days=-1)]}))


def test_insert_errors(sql):

    table_name = '##test_insert_errors'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
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

    with pytest.raises(errors.SQLInvalidInsertFormat):
        sql.write.insert(table_name, dataframe=pd.DataFrame({'ColumnD': ['06/22/2021']}), include_timestamps=False)


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


def test_insert_exclude_timestamps(sql):

    table_name = '##test_insert_exclude_timestamps'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT'
    })
    dataframe = pd.DataFrame({"ColumnA": [1,2,3]})
    sql.write.insert(table_name, dataframe, include_timestamps=False)
    results = sql.read.select(table_name)
    assert all(results==dataframe)


def test__prep_update_merge(sql):

    table_name = '##test__prep_update_merge'
    sql.create.table(table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'INT'
    })

    dataframe = pd.DataFrame({'ColumnA': [1]})

    with pytest.raises(errors.SQLUndefinedPrimaryKey):
        sql.write._write__prep_update_merge(table_name, match_columns=None, dataframe=dataframe, operation='update')

    with pytest.raises(errors.SQLColumnDoesNotExist):
        sql.write._write__prep_update_merge(table_name, match_columns='MissingColumn', dataframe=dataframe, operation='update')       

    with pytest.raises(errors.DataframeUndefinedColumn):
        sql.write._write__prep_update_merge(table_name, match_columns='ColumnB', dataframe=dataframe, operation='update')   


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
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
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
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='sql')
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


def test_update_exclude_timestamps(sql):

    table_name = '##test_update_exclude_timestamps'
    dataframe = pd.DataFrame({
        'ColumnA': [1,2],
        'ColumnB': ['a','b'],
        'ColumnC': [3,4]
    })
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
    sql.write.insert(table_name, dataframe, include_timestamps=False)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe['ColumnC'] = [5,6]
    sql.write.update(table_name, dataframe[['ColumnC']], include_timestamps=False)
    result = sql.read.select(table_name)
    assert all(result[['ColumnA','ColumnB','ColumnC']]==dataframe[['ColumnA','ColumnB','ColumnC']])
    assert '_time_update' not in result.columns
    assert '_time_insert' not in result.columns


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
    
    table_name = "##test_merge_keep_unmatched"
    dataframe = pd.DataFrame({
        'ColumnA': [3,4]
    })
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
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
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
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
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
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


def test_merge_one_delete_condition(sql):
    
    table_name = "##test_merge_one_delete_condition"
    dataframe = pd.DataFrame({
        'State': ['A','B','B'],
        'ColumnA': [3,4,4],
        'ColumnB': ['a','b','b']
    }, index=[0,1,2])
    dataframe.index.name='_pk'
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
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
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
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
    sql.create.table_from_dataframe(table_name, dataframe, primary_key='index')
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