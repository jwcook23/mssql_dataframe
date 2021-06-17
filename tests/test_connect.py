import pyodbc
import pytest

from mssql_dataframe import connect


def test_SQLServer_trusted():
    '''Test connection using Windows authentication.'''
    db = connect.SQLServer(database_name='master', server_name='localhost')
    assert isinstance(db.connection, pyodbc.Connection)

def test_SQLServer_credentials():
    '''Test attempted connection using username/password without having to hardcode credentials.'''
    with pytest.raises(pyodbc.InterfaceError):
        connect.SQLServer(database_name='master', server_name='localhost', username='admin', password='')

def test_SQLServer_fail():
    '''Test specifying an invalid driver name.'''
    with pytest.raises(connect.ODBCDriverNotFound):
        connect.SQLServer(database_name='master', server_name='localhost', driver='')