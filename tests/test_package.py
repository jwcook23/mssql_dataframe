import env
import warnings
import logging

import pandas as pd

import mssql_dataframe
from mssql_dataframe.package import SQLServer
from mssql_dataframe.core import custom_warnings


attributes = [
    "_conn",
    "connection",
    "_versions",
    "exceptions",
    "create",
    "modify",
    "read",
    "write",
]


def test_version():
    assert isinstance(mssql_dataframe.__version__, str)
    assert len(mssql_dataframe.__version__) > 0


def test_SQLServer_basic():

    with warnings.catch_warnings(record=True) as warn:
        assert len(warn) == 0
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


def test_SQLServer_timestamps():

    with warnings.catch_warnings(record=True) as warn:
        sql = SQLServer(
            env.database,
            env.server,
            env.driver,
            env.username,
            env.password,
            include_metadata_timestamps=True,
        )
        assert len(warn) == 1
        assert isinstance(warn[-1].message, custom_warnings.SQLObjectAdjustment)
        assert (
            str(warn[0].message)
            == "SQL write operations will include metadata '_time_insert' & '_time_update' columns as 'include_metadata_timestamps=True'."
        )
        assert isinstance(sql, SQLServer)
        assert list(vars(sql).keys()) == attributes


def test_SQLServer_autoadjust():

    with warnings.catch_warnings(record=True) as warn:
        sql = SQLServer(
            env.database,
            env.server,
            env.driver,
            env.username,
            env.password,
            autoadjust_sql_objects=True,
        )
        assert len(warn) == 1
        assert isinstance(warn[-1].message, custom_warnings.SQLObjectAdjustment)
        assert (
            str(warn[0].message)
            == "SQL objects will be created/modified as needed as 'autoadjust_sql_objects=True'."
        )
        assert isinstance(sql, SQLServer)
        assert list(vars(sql).keys()) == attributes


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
        assert len(caplog.record_tuples) == 2

        assert caplog.record_tuples[0][0] == "root"
        assert caplog.record_tuples[0][1] == logging.DEBUG
        assert caplog.record_tuples[0][2].startswith("Connection Info:")
        assert caplog.record_tuples[1][0] == "root"
        assert caplog.record_tuples[1][1] == logging.DEBUG
        assert caplog.record_tuples[1][2].startswith("Version Numbers:")

        assert isinstance(sql._conn, dict)
        assert isinstance(sql._versions, dict)


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
