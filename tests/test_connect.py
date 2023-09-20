import env

import pyodbc
import pytest

from mssql_dataframe.connect import connect


def test_trusted():
    # master database, local host, trusted Windows connection
    db = connect(
        database=env.database, 
        server=env.server, 
        driver=env.driver, 
        trusted_connection="yes"
    )
    assert isinstance(db.connection, pyodbc.Connection)


def test_exceptions():

    # connection info for pyodbc must be supplied
    with pytest.raises(pyodbc.OperationalError):
        connect()

    # username/password without having to hardcode for testing
    with pytest.raises(pyodbc.InterfaceError):
        connect(
            database=env.database,
            server=env.server,
            UID="admin",
            PWD="",
        )

    # invalid driver name
    with pytest.raises(pyodbc.InterfaceError):
        connect(database=env.database, server=env.server, driver="", trusted_connection="yes")
