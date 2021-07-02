import pyodbc
import pytest

from mssql_dataframe import errors, connect


def test_SQLServer():
    
    # trusted Windows connection
    db = connect.SQLServer(database_name='master', server_name='localhost')
    assert isinstance(db.connection, pyodbc.Connection)

    # username/password without having to hardcode for testing
    with pytest.raises(pyodbc.InterfaceError):
        connect.SQLServer(database_name='master', server_name='localhost', username='admin', password='')
    
    # invalid driver name
    with pytest.raises(errors.ODBCDriverNotFound):
        connect.SQLServer(database_name='master', server_name='localhost', driver='')


def test_AzureSQL():

    with pytest.raises(NotImplementedError):
        connect.AzureSQL()