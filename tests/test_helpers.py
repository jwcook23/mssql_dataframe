import pytest
import pandas as pd

from mssql_dataframe import connect
from mssql_dataframe import helpers
from mssql_dataframe import errors


@pytest.fixture(scope="module")
def connection():

    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


def test_safe_sql(connection):

    inputs = ["TableName","##TableName","ColumnName","'; select true; --", "abc[]def", "user's custom name"]
    clean = helpers.safe_sql(connection, inputs)
    assert len(clean)==len(inputs)

    inputs = "SingleString"
    clean = helpers.safe_sql(connection, inputs)
    assert len(clean)==1

    dataframe = pd.DataFrame(columns=["A","B"])
    clean = helpers.safe_sql(connection, dataframe.columns)
    assert len(clean)==dataframe.shape[1]


def test_column_spec(connection):

    columns = ['VARCHAR', 'VARCHAR(MAX)', 'VARCHAR(200)', 'INT', 'DECIMAL(5,2)']
    size, dtypes = helpers.column_spec(columns)

    assert size==[None, '(MAX)', '(200)', None, '(5,2)']
    assert dtypes==['VARCHAR', 'VARCHAR', 'VARCHAR', 'INT', 'DECIMAL']


def test_infer_datatypes(connection):
    pass


def test_get_schema(connection):

    table_name = '##test_get_schema'
    with pytest.raises(errors.TableDoesNotExist):
        helpers.get_schema(connection, table_name)