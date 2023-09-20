import env
import logging

import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create, conversion
from mssql_dataframe.core.write import insert, update
from mssql_dataframe.__equality__ import compare_dfs

pd.options.mode.chained_assignment = "raise"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.insert = insert.insert(self.connection)
        self.update = update.update(self.connection)
        self.update_meta = update.update(
            self.connection, include_metadata_timestamps=True
        )


@pytest.fixture(scope="module")
def sql():
    db = connect(database=env.database, server=env.server, trusted_connection="yes")
    yield package(db)
    db.connection.close()


def test_update_primary_key(sql, caplog):
    table_name = "##test_update_primary_key"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]},
        index=pd.Series([0, 1], name="_index"),
    )
    sql.create.table(
        table_name,
        {
            "ColumnA": "TINYINT",
            "ColumnB": "CHAR(1)",
            "ColumnC": "TINYINT",
            "_index": "TINYINT",
        },
        primary_key_column="_index",
    )
    dataframe = sql.insert.insert(table_name, dataframe)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe["ColumnC"] = [5, 6]
    updated = sql.update.update(table_name, dataframe=dataframe[["ColumnC"]])
    dataframe["ColumnC"] = updated["ColumnC"]

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert compare_dfs(dataframe, result[dataframe.columns])
    assert "_time_update" not in result.columns
    assert "_time_insert" not in result.columns

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 0


def test_update_override_timestamps(sql, caplog):
    table_name = "##test_update_override_timestamps"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]},
        index=pd.Series([0, 1], name="_index"),
    )
    sql.create.table(
        table_name,
        {
            "ColumnA": "TINYINT",
            "ColumnB": "CHAR(1)",
            "ColumnC": "TINYINT",
            "_index": "TINYINT",
        },
        primary_key_column="_index",
    )
    dataframe = sql.insert.insert(table_name, dataframe)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe["ColumnC"] = [5, 6]
    updated = sql.update.update(
        table_name, dataframe=dataframe[["ColumnC"]], include_metadata_timestamps=True
    )
    dataframe["ColumnC"] = updated["ColumnC"]

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert compare_dfs(dataframe, result[dataframe.columns])
    assert result["_time_update"].notna().all()

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert (
        caplog.record_tuples[0][2]
        == f"Creating column '_time_update' in table '{table_name}' with data type 'datetime2'."
    )


def test_update_nonpk_column(sql, caplog):
    table_name = "##test_update_nonpk_column"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]}
    )
    sql.create.table(
        table_name, {"ColumnA": "TINYINT", "ColumnB": "CHAR(1)", "ColumnC": "TINYINT"}
    )
    dataframe = sql.insert.insert(table_name, dataframe)

    # update values in table, using the SQL primary key that came from the dataframe's index
    dataframe["ColumnB"] = ["c", "d"]
    updated = sql.update.update(
        table_name, dataframe=dataframe[["ColumnB", "ColumnC"]], match_columns="ColumnC"
    )
    dataframe["ColumnB"] = updated["ColumnB"]

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert compare_dfs(dataframe, result[dataframe.columns])
    assert "_time_update" not in result.columns
    assert "_time_insert" not in result.columns

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 0


def test_update_two_match_columns(sql, caplog):
    table_name = "##test_update_two_match_columns"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]}
    )
    sql.create.table(
        table_name,
        {"ColumnA": "TINYINT", "ColumnB": "CHAR(1)", "ColumnC": "TINYINT"},
        sql_primary_key=True,
    )
    dataframe = sql.insert.insert(table_name, dataframe)

    # update values in table, using the primary key created in SQL and ColumnA
    schema, _ = conversion.get_schema(sql.connection, table_name)
    dataframe = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    dataframe["ColumnC"] = [5, 6]
    updated = sql.update_meta.update(
        table_name, dataframe, match_columns=["_pk", "ColumnA"]
    )

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert compare_dfs(updated, result[updated.columns])
    assert result["_time_update"].notna().all()

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert (
        caplog.record_tuples[0][2]
        == f"Creating column '_time_update' in table '{table_name}' with data type 'datetime2'."
    )


def test_update_composite_pk(sql, caplog):
    table_name = "##test_update_composite_pk"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]}
    ).set_index(["ColumnA", "ColumnB"])
    sql.create.table(
        table_name,
        {"ColumnA": "TINYINT", "ColumnB": "CHAR(1)", "ColumnC": "TINYINT"},
        primary_key_column=["ColumnA", "ColumnB"],
    )
    dataframe = sql.insert.insert(table_name, dataframe)

    # update values in table, using the primary key created in SQL and ColumnA
    dataframe["ColumnC"] = [5, 6]
    updated = sql.update.update(table_name, dataframe)

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert compare_dfs(result, updated)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 0
