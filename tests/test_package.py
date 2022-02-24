import env
import logging

import pandas as pd

import mssql_dataframe
from mssql_dataframe.package import SQLServer


attributes = [
    "connection_spec",
    "connection",
    "version_spec",
    "exceptions",
    "create",
    "modify",
    "read",
    "write",
]


def test_version():
    assert isinstance(mssql_dataframe.__version__, str)
    assert len(mssql_dataframe.__version__) > 0


def test_SQLServer_basic(caplog):

    sql = SQLServer(
        env.database,
        env.server,
        env.driver,
        env.username,
        env.password,
        autoadjust_sql_objects=False,
    )
    assert isinstance(sql, SQLServer)
    assert list(vars(sql).keys()) == attributes

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 0


def test_SQLServer_timestamps(caplog):

    sql = SQLServer(
        env.database,
        env.server,
        env.driver,
        env.username,
        env.password,
        include_metadata_timestamps=True,
    )

    assert isinstance(sql, SQLServer)
    assert list(vars(sql).keys()) == attributes

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.package"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert (
        caplog.record_tuples[0][2]
        == "SQL write operations will include metadata '_time_insert' & '_time_update' columns as 'include_metadata_timestamps=True'."
    )


def test_SQLServer_autoadjust(caplog):

    sql = SQLServer(
        env.database,
        env.server,
        env.driver,
        env.username,
        env.password,
        autoadjust_sql_objects=True,
    )

    assert isinstance(sql, SQLServer)
    assert list(vars(sql).keys()) == attributes

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.package"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert (
        caplog.record_tuples[0][2]
        == "SQL objects will be created/modified as needed as 'autoadjust_sql_objects=True'."
    )


def test_SQLServer_log_init(caplog):

    with caplog.at_level(logging.DEBUG):

        sql = SQLServer(
            env.database,
            env.server,
            env.driver,
            env.username,
            env.password,
            autoadjust_sql_objects=False,
        )
        assert isinstance(sql.connection_spec, dict)
        assert isinstance(sql.version_spec, dict)

        # assert warnings raised by logging after all other tasks
        assert len(caplog.record_tuples) == 2
        assert caplog.record_tuples[0][0] == "mssql_dataframe.package"
        assert caplog.record_tuples[0][1] == logging.DEBUG
        assert caplog.record_tuples[0][2].startswith("Connection Info:")
        assert caplog.record_tuples[1][0] == "mssql_dataframe.package"
        assert caplog.record_tuples[1][1] == logging.DEBUG
        assert caplog.record_tuples[1][2].startswith("Version Numbers:")


def test_SQLServer_schema():

    table_name = "##test_SQLServer_schema"
    sql = SQLServer(
        env.database,
        env.server,
        env.driver,
        env.username,
        env.password,
        autoadjust_sql_objects=False,
    )
    sql.create.table(table_name, columns={"ColumnA": "bigint"})

    schema = sql.get_schema(table_name)
    assert schema.index.equals(pd.Index(["ColumnA"], dtype="string"))
