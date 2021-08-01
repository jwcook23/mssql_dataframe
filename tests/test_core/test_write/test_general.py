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


def test_allowable_failures(sql):

    table_name = '##test_allowable_failures'
    sql.create.table(
        table_name,
        columns={'ColumnA': 'TINYINT'},
        primary_key_column='ColumnA', 
        not_null='ColumnA'
    )

    with pytest.raises(pyodbc.IntegrityError):
        sql.write.insert(table_name, pd.DataFrame({'ColumnA': [1,1]}), include_timestamps=False)
    with pytest.raises(pyodbc.IntegrityError):
        sql.write.insert(table_name, pd.DataFrame({'ColumnA': [1,None]}), include_timestamps=False)


def test__prepare_values(sql):

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
    assert all(dataframe['ColumnA'].values==['a  ','  b  ','c',None,None])
    assert all(dataframe['ColumnB'].values==['01:00:00.1234567']*5)
    assert all(dataframe['ColumnC'].values==['00:00:00.123400']*5)

    with pytest.raises(ValueError):
        sql.write._write__prepare_values(pd.DataFrame({'Column': [pd.Timedelta(days=1)]}))

    with pytest.raises(ValueError):
        sql.write._write__prepare_values(pd.DataFrame({'Column': [pd.Timedelta(days=-1)]}))


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
