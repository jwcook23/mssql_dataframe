import warnings

import pandas as pd

from mssql_dataframe.package import SQLServer
from mssql_dataframe.core import custom_warnings

attributes = ["connection", "exceptions", "create", "modify", "read", "write"]


def test_SQLServer():

    # autoadjust_sql_objects==False
    with warnings.catch_warnings(record=True) as warn:
        assert len(warn) == 0
        sql = SQLServer(autoadjust_sql_objects=False)
        assert isinstance(sql, SQLServer)
        assert list(vars(sql).keys()) == attributes

    # include_metadata_timestamps==True
    with warnings.catch_warnings(record=True) as warn:
        adjustable = SQLServer(include_metadata_timestamps=True)
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
        adjustable = SQLServer(autoadjust_sql_objects=True)
        assert len(warn) == 1
        assert isinstance(warn[-1].message, custom_warnings.SQLObjectAdjustment)
        assert (
            str(warn[0].message)
            == "SQL objects will be created/modified as needed as autoadjust_sql_objects=True"
        )
        assert isinstance(adjustable, SQLServer)
        assert list(vars(sql).keys()) == attributes


def test_SQLServer_schema():

    table_name = "##test_SQLServer_schema"
    sql = SQLServer(autoadjust_sql_objects=False)
    sql.create.table(table_name, columns={"ColumnA": "bigint"})

    schema = sql.get_schema(table_name)
    assert schema.index.equals(pd.Index(["ColumnA"]))
