import pytest
import pandas as pd
import numpy as np

from mssql_dataframe import connect
from mssql_dataframe import write
from mssql_dataframe import read
from mssql_dataframe import create

@pytest.fixture(scope="module")
def connection():

    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


def test_select(connection):

    # create table and insert sample data
    table_name = '##test_select'
    create.table(connection, table_name, columns={
            'ColumnA': 'TINYINT',
            'ColumnB': 'INT',
            'ColumnC': 'BIGINT',
            'ColumnD': 'DATETIME',
            'ColumnE': 'VARCHAR(10)'
    })

    input = pd.DataFrame({
        'ColumnA': [5,6,7],
        'ColumnB': [5,6,None],
        'ColumnC': [pd.NA,6,7],
        'ColumnD': ['06-22-2021','06-22-2021',pd.NaT],
        'ColumnE' : ['a','b',None]
    })
    input['ColumnB'] = input['ColumnB'].astype('Int64')
    input['ColumnD'] = pd.to_datetime(input['ColumnD'])
    write.insert(connection, table_name, input)

    # all columns and rows
    dataframe = read.select(connection, table_name)
    assert all(dataframe.columns==input.columns)
    assert dataframe.shape==input.shape
    assert dataframe.dtypes['ColumnA']=='Int8'
    assert dataframe.dtypes['ColumnB']=='Int32'
    assert dataframe.dtypes['ColumnC']=='Int64'
    assert dataframe.dtypes['ColumnD']=='datetime64[ns]'
    assert dataframe.dtypes['ColumnE']=='object'

    # # columns
    dataframe = read.select(connection, table_name, column_names=["ColumnA","ColumnB"])
    assert all(dataframe.columns==["ColumnA","ColumnB"])
    assert dataframe.shape[0]==input.shape[0]

    # where
    dataframe = read.select(connection, table_name, column_names=['ColumnB','ColumnC','ColumnD'], where="ColumnB>4 AND ColumnC IS NOT NULL OR ColumnD IS NULL")
    assert sum((dataframe['ColumnB']>4 & dataframe['ColumnC'].notna()) | dataframe['ColumnD'].isna())==2

    # limit
    dataframe = read.select(connection, table_name, limit=1)
    assert dataframe.shape[0]==1

    # order
    dataframe = read.select(connection, table_name, column_names=["ColumnA"], order_column='ColumnA', order_direction='DESC')
    assert(all(dataframe['ColumnA']==[7,6,5]))