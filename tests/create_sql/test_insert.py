import pytest

from mssql_dataframe.create_sql.connection import SQLServer, AzureSQLDatabase
from mssql_dataframe.create_sql.table import table
from mssql_dataframe.create_sql.insert import insert

from tests.create_sql import data


@pytest.fixture(scope="module")
def connection():
    db = SQLServer(database_name='master', server_name='localhost', autocommit=False)
    sql = {'table': table(db), 'insert': insert(db)}
    yield sql
    db.connection.close()


@pytest.fixture(scope="module")
def sample():
    return data.sample()


def test_insert_value(connection, sample):

    name = '##test_insert_value'
    columns = {sample.pk: sample.columns[sample.pk]}
    dataframe = sample.dataframe[[sample.pk]].head(1)

    connection['table'].create_table(name, columns)
    
    connection['insert'].insert_data(name, dataframe)


def test_insert_column(connection, sample):

    name = '##test_insert_column'
    columns = {sample.pk: sample.columns[sample.pk]}
    dataframe = sample.dataframe[[sample.pk]]

    connection['table'].create_table(name, columns)
    
    connection['insert'].insert_data(name, dataframe)


def test_insert_dataframe(connection, sample):

    name = '##test_insert_dataframe'
    columns = sample.columns
    dataframe = sample.dataframe

    connection['table'].create_table(name, columns)
    
    connection['insert'].insert_data(name, dataframe)


def test_from_dataframe_autopk(connection, sample):

    name = '##test_from_dataframe_autopk'

    dataframe = sample.dataframe
    dataframe.index.name = None

    table.from_dataframe(name, dataframe)

