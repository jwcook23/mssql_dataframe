import pytest

from mssql_dataframe.create_sql import connect
from mssql_dataframe.create_sql import table

from tests.create_sql import data


@pytest.fixture(scope="module")
def connection():
    db = connect.SQLServer(database_name='master', server_name='localhost', autocommit=False)
    sql = table.table(db)
    yield sql
    db.connection.close()


@pytest.fixture(scope="module")
def data_simple():
    return data.simple()


@pytest.fixture(scope="module")
def data_complex():
    return data.complex()


def test_create_table_column(connection, data_complex):

    name = '#test_create_table_column'
    columns = {data_complex.pk: data_complex.columns['_varchar']}

    connection.create_table(name, columns)


def test_create_table_pk(connection, data_complex):

    name = "#test_create_table_pk"
    columns = {data_complex.pk: data_complex.columns[data_complex.pk]}

    primary_key = data_complex.pk

    connection.create_table(name, columns, primary_key)


def test_create_table_columns(connection, data_complex):

    name = '#test_create_table_dataframe'
    columns = data_complex.columns

    primary_key = data_complex.pk
    notnull = data_complex.notnull

    connection.create_table(name, columns, primary_key, notnull)


def test_from_dataframe_simple_nopk(connection, data_simple):

    name = '##test_from_dataframe_simple_nopk'

    dataframe = data_simple.dataframe

    connection.from_dataframe(name, dataframe, primary_key=None)

# def test_from_dataframe_nopk(connection, data_complex):
# def test_from_dataframe_autopk(connection, data_complex):
# def test_from_dataframe_indexpk(connection, data_complex):