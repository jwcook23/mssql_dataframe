import env
import warnings

import pandas as pd

import mssql_dataframe
from mssql_dataframe.package import SQLServer
from mssql_dataframe.core import custom_warnings


def test_version():
    assert isinstance(mssql_dataframe.__version__, str)
    assert len(mssql_dataframe.__version__) > 0


def test_SQLServer():

    attributes = [
        "_conn",
        "connection",
        "exceptions",
        "create",
        "modify",
        "read",
        "write",
    ]

    # autoadjust_sql_objects==False
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

    # include_metadata_timestamps==True
    with warnings.catch_warnings(record=True) as warn:
        adjustable = SQLServer(
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
            == "SQL write operations will include metadata _time_insert & time_update columns as include_metadata_timestamps=True"
        )
        assert isinstance(adjustable, SQLServer)
        assert list(vars(sql).keys()) == attributes

    # autoadjust_sql_objects==True
    with warnings.catch_warnings(record=True) as warn:
        adjustable = SQLServer(
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
            == "SQL objects will be created/modified as needed as autoadjust_sql_objects=True"
        )
        assert isinstance(adjustable, SQLServer)
        assert list(vars(sql).keys()) == attributes

    # output debug info
    sql.output_debug()
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
