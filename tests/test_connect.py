import env

import pyodbc
import pytest

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_errors


def test_connect():
    # master database, local host, trusted Windows connection
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    assert isinstance(db.connection, pyodbc.Connection)

    # username/password without having to hardcode for testing
    with pytest.raises(pyodbc.InterfaceError):
        connect(
            env.database,
            env.server,
            username="admin",
            password="",
        )

    # invalid driver name
    with pytest.raises(custom_errors.EnvironmentODBCDriverNotFound):
        connect(env.database, env.server, driver="")
