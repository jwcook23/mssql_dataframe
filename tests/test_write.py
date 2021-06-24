import pytest
import pandas as pd
import numpy as np
from datetime import date

from mssql_dataframe import connect
from mssql_dataframe import write
from mssql_dataframe import create

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

    results = connection.cursor.execute('SELECT * FROM {table_name}'.format(table_name=table_name)).fetchall()
    assert list(results[0])==[1, None, None, None, None]
    assert list(results[1])==[None, 2, None, None, None]
    assert list(results[2])==[None, 3, None, None, None]
    assert list(results[3])==[None, 4, None, None, None]
    assert list(results[4])==[5, 5, None, date(2021,6,22), 'a']
    assert list(results[5])==[None, 6, 6, date(2021,6,22), 'b']
    assert list(results[6])==[7, None, 7, None, None]