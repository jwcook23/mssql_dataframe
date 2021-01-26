import pytest
import pandas as pd

from mssql_dataframe.core.backend import SQLServer
from mssql_dataframe.core import execute_statement


@pytest.fixture(scope="module")
def connection():
    db = SQLServer(database_name='master', server_name='localhost')
    yield db
    db.engine.close()


def test_create_table_simple(connection):

    name = '#create_test_simple'
    columns = {'TESTA': 'INT'}

    statement, args = execute_statement.create_table(name, columns)

    connection.engine.execute(statement, *args)


def test_create_table_pk(connection):

    name = "#create_table_pk"
    columns = {'TESTA': 'BIGINT'}
    primary_key = 'TESTA'

    statement, args = execute_statement.create_table(name, columns, primary_key)

    connection.engine.execute(statement, *args)


def test_create_table_complex(connection):

    name = '#create_table_complex'
    columns = {'TESTA': 'NVARCHAR(100)', 'TESTB': 'INT'}
    primary_key = 'TESTA'
    notnull = ['TESTB']

    statement, args = execute_statement.create_table(name, columns, primary_key, notnull)

    connection.engine.execute(statement, *args)


def test_insert_data_simple(connection):

    name = '##insert_data_simple'
    columns = {'TESTA': 'INT'}

    statement, args = execute_statement.create_table(name, columns)

    connection.engine.execute(statement, *args)

    data = pd.DataFrame({'TESTA': [1,2,3,4,5]})

    statement, values = execute_statement.insert_data(name, data)

    connection.engine.executemany(statement, values)
