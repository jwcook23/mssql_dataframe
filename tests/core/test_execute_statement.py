from datetime import datetime, date

import pytest
import pandas as pd

from mssql_dataframe.core.backend import SQLServer
from mssql_dataframe.core import execute_statement


@pytest.fixture(scope="module")
def connection():
    db = SQLServer(database_name='master', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


@pytest.fixture(scope="module")
def data():

    class values():
        dataframe = pd.DataFrame({
            '_varchar': [None,'b','c','d','e'],
            '_tinyint': [None,2,3,4,5],
            '_int': [1,2,3,4,5],
            '_bigint': [1,2,3,4,9999999999],
            '_numeric': [1.11,2,3,4,None],
            '_float': [1.111111,2,3,4,5],
            '_date': [date.today()]*5,
            '_time': [datetime.now().time()]*5,
            '_datetime': [datetime.now()]*4+[pd.NaT]  
        })
        dataframe['_tinyint'] = dataframe['_tinyint'].astype('Int64')

        columns  = {
            '_varchar': 'VARCHAR(255)',
            '_tinyint': 'TINYINT',
            '_int': 'INT',
            '_bigint': 'BIGINT',
            '_numeric': 'NUMERIC(20,4)',
            '_float': 'FLOAT',
            '_date': 'DATE',
            '_time': 'TIME',
            '_datetime': 'DATETIME'         
        }

        pk = '_int'

        notnull = ['_bigint','_float']

    return values


def test_create_table_column(connection, data):

    table = '#test_create_table_column'
    columns = {data.pk: data.columns[data.pk]}

    statement, args = execute_statement.create_table(table, columns)

    connection.cursor.execute(statement, *args)


def test_create_table_pk(connection, data):

    table = "#test_create_table_pk"
    columns = {data.pk: data.columns[data.pk]}

    primary_key = data.pk

    statement, args = execute_statement.create_table(table, columns, primary_key)

    connection.cursor.execute(statement, *args)


def test_create_table_dataframe(connection, data):

    table = '#test_create_table_dataframe'
    columns = data.columns

    primary_key = data.pk
    notnull = data.notnull

    statement, args = execute_statement.create_table(table, columns, primary_key, notnull)

    connection.cursor.execute(statement, *args)


def test_insert_data_value(connection, data):

    table = '##test_insert_data_value'
    columns = {data.pk: data.columns[data.pk]}
    df = data.dataframe[[data.pk]].head(1)

    statement, args = execute_statement.create_table(table, columns)

    connection.cursor.execute(statement, *args)
    
    statement, values = execute_statement.insert_data(table, df)

    connection.cursor.executemany(statement, values)
