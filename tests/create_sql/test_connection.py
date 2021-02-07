import pyodbc

from mssql_dataframe.create_sql import connection


def test_SQLServer():

    db = connection.SQLServer(database_name='master', server_name='localhost')

    assert isinstance(db.connection, pyodbc.Connection)


# TODO: test AzureSQLDatabase
# def test_AzureSQLDatabase

    # db = connection.AzureSQLDatabase()