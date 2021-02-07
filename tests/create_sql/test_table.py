import pytest

from mssql_dataframe.create_sql.connection import SQLServer, AzureSQLDatabase
from mssql_dataframe.create_sql.table import table

from tests.create_sql import data


@pytest.fixture(scope="module")
def connection():
    db = SQLServer(database_name='master', server_name='localhost', autocommit=False)
    sql = table(db)
    yield sql
    db.connection.close()


@pytest.fixture(scope="module")
def sample():
    return data.sample()


def test_create_table_column(connection, sample):

    name = '#test_create_table_column'
    columns = {sample.pk: sample.columns[sample.pk]}

    connection.create_table(name, columns)


def test_create_table_pk(connection, sample):

    name = "#test_create_table_pk"
    columns = {sample.pk: sample.columns[sample.pk]}

    primary_key = sample.pk

    connection.create_table(name, columns, primary_key)


def test_create_table_dataframe(connection, sample):

    name = '#test_create_table_dataframe'
    columns = sample.columns

    primary_key = sample.pk
    notnull = sample.notnull

    connection.create_table(name, columns, primary_key, notnull)


def test_from_dataframe_namedpk(connection, sample):

    name = '##test_from_dataframe_namedpk'

    dataframe = sample.dataframe
    dataframe.index.name = '_pk'

    connection.from_dataframe(name, dataframe)

