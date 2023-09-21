import env

import pyodbc
import pytest

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_errors


# override conftest.py add_docstring_namespace fixture locally
# prevents initializing SQLServer for tests in this file
@pytest.fixture(autouse=True)
def add_docstring_namespace():
    return


def test_trusted():
    # master database, local host, trusted Windows connection
    db = connect(
        database=env.database,
        server=env.server,
        driver=env.driver,
        trusted_connection="yes",
        TrustServerCertificate="yes",
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
    with pytest.raises(custom_errors.EnvironmentODBCDriverNotFound):
        connect(
            database=env.database,
            server=env.server,
            driver="",
            trusted_connection="yes",
        )
