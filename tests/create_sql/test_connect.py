import pyodbc

from mssql_dataframe.create_sql import connect


def test_SQLServer():

    db = connect.SQLServer(database_name='master', server_name='localhost')

    assert isinstance(db.connection, pyodbc.Connection)


# TODO: test AzureSQLDatabase