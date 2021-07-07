import pyodbc
import pytest

from mssql_dataframe import errors, connect


def test_connect():

    # master database, local host, trusted Windows connection
    db = connect.connect()
    assert isinstance(db.connection, pyodbc.Connection)

    # username/password without having to hardcode for testing
    with pytest.raises(pyodbc.InterfaceError):
        connect.connect(database_name='master', server_name='localhost', username='admin', password='')
    
    # invalid driver name
    with pytest.raises(errors.ODBCDriverNotFound):
        connect.connect(database_name='master', server_name='localhost', driver='')
