import env
import logging

import pytest
import pandas as pd
import pyodbc

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create, conversion
from mssql_dataframe.core.write import insert, update, merge
from mssql_dataframe.__equality__ import compare_dfs

pd.options.mode.chained_assignment = "raise"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.insert = insert.insert(self.connection)
        self.update = update.update(self.connection)
        self.merge_meta = merge.merge(self.connection, include_metadata_timestamps=True)


@pytest.fixture(scope="module")
def sql():
    db = connect(database=env.database, server=env.server, trusted_connection="yes")
    yield package(db)
    db.connection.close()


def test_update_nondbo_schema(sql, caplog):
    schema_name = "foo"
    table_name = "test_update_nondbo_schema"
    combined_name = f"{schema_name}.{table_name}"
    cursor = sql.connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA {schema_name};")
    except pyodbc.ProgrammingError:
        pass
    cursor.execute(f"DROP TABLE IF EXISTS {combined_name}")
    cursor.commit()

    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]},
        index=pd.Series([0, 1], name="_index"),
    )
    sql.create.table(
        combined_name,
        {
            "ColumnA": "TINYINT",
            "ColumnB": "CHAR(1)",
            "ColumnC": "TINYINT",
            "_index": "TINYINT",
        },
        primary_key_column="_index",
    )

    dataframe = sql.insert.insert(combined_name, dataframe)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe["ColumnC"] = [5, 6]
    updated = sql.update.update(combined_name, dataframe=dataframe[["ColumnC"]])
    dataframe["ColumnC"] = updated["ColumnC"]

    # test result
    schema, _ = conversion.get_schema(sql.connection, combined_name)
    result = conversion.read_values(
        f"SELECT * FROM {combined_name}", schema, sql.connection
    )
    assert compare_dfs(dataframe, result[dataframe.columns])
    assert "_time_update" not in result.columns
    assert "_time_insert" not in result.columns

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 0


def test_merge_nondbo_schema(sql, caplog):
    schema_name = "bar"
    table_name = "test_merge_nondbo_schema"
    combined_name = f"{schema_name}.{table_name}"
    cursor = sql.connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA {schema_name};")
    except pyodbc.ProgrammingError:
        pass
    cursor.execute(f"DROP TABLE IF EXISTS {combined_name}")
    cursor.commit()

    dataframe = pd.DataFrame(
        {"ColumnA": [3, 4]}, index=pd.Series([0, 1], name="_index")
    )
    sql.create.table(
        combined_name,
        {"ColumnA": "TINYINT", "_index": "TINYINT"},
        primary_key_column="_index",
    )
    dataframe = sql.insert.insert(combined_name, dataframe)

    # delete
    dataframe = dataframe[dataframe.index != 0]
    # update
    dataframe.loc[dataframe.index == 1, "ColumnA"] = 5
    # insert
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame([6], columns=["ColumnA"], index=pd.Index([2], name="_index")),
        ]
    )

    # merge values into table, using the SQL primary key that came from the dataframe's index
    dataframe = sql.merge_meta.merge(combined_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, combined_name)
    result = conversion.read_values(
        f"SELECT * FROM {combined_name}", schema, sql.connection
    )
    assert compare_dfs(result[dataframe.columns], dataframe)
    assert all(result["_time_update"].notna() == [True, False])
    assert all(result["_time_insert"].notna() == [False, True])

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 2
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert (
        caplog.record_tuples[0][2]
        == f"Creating column '_time_update' in table '{combined_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[1][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[1][1] == logging.WARNING
    assert (
        caplog.record_tuples[1][2]
        == f"Creating column '_time_insert' in table '{combined_name}' with data type 'datetime2'."
    )
