import pytest

# from mssql_dataframe.create_sql.connection import SQLServer, AzureSQLDatabase
# from mssql_dataframe.create_sql.table import table
# from mssql_dataframe.create_sql.insert import insert
# from mssql_dataframe import create_sql
from mssql_dataframe.create_sql import connect
from mssql_dataframe.create_sql import table
from mssql_dataframe.create_sql import insert

from tests.create_sql import data


@pytest.fixture(scope="module")
def connection():
    db = connect.SQLServer(database_name='master', server_name='localhost', autocommit=False)
    sql = {'table': table.table(db), 'insert': insert.insert(db)}
    yield sql
    db.connection.close()


@pytest.fixture(scope="module")
def data_simple():
    return data.simple()

@pytest.fixture(scope="module")
def data_complex():
    return data.complex()


def test_insert_value(connection, data_complex):

    name = '##test_insert_value'
    columns = {data_complex.pk: data_complex.columns[data_complex.pk]}
    dataframe = data_complex.dataframe[[data_complex.pk]].head(1)

    connection['table'].create_table(name, columns)
    
    connection['insert'].insert_data(name, dataframe)


def test_insert_column(connection, data_complex):

    name = '##test_insert_column'
    columns = {data_complex.pk: data_complex.columns[data_complex.pk]}
    dataframe = data_complex.dataframe[[data_complex.pk]]

    connection['table'].create_table(name, columns)
    
    connection['insert'].insert_data(name, dataframe)


def test_insert_dataframe(connection, data_complex):

    name = '##test_insert_dataframe'
    columns = data_complex.columns
    dataframe = data_complex.dataframe

    connection['table'].create_table(name, columns)
    
    connection['insert'].insert_data(name, dataframe)