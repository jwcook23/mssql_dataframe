import env
import logging

import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create, conversion
from mssql_dataframe.core.write import insert

pd.options.mode.chained_assignment = "raise"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.insert = insert.insert(self.connection)
        self.insert_meta = insert.insert(
            self.connection, include_metadata_timestamps=True
        )


@pytest.fixture(scope="module")
def sql():
    db = connect(database=env.database, server=env.server, trusted_connection="yes")
    yield package(db)
    db.connection.close()


def test_insert_singles(sql):
    table_name = "##test_insert_singles"

    # create table
    columns = {
        "ColumnA": "TINYINT",
        "ColumnB": "INT",
        "ColumnC": "DATE",
    }
    sql.create.table(table_name, columns)

    schema, _ = conversion.get_schema(sql.connection, table_name)

    # single value
    dataframe = pd.DataFrame({"ColumnA": [1]})
    dataframe = sql.insert.insert(table_name, dataframe)
    result = conversion.read_values(
        f"SELECT ColumnA FROM {table_name}", schema, sql.connection
    )
    assert all(result["ColumnA"] == [1])

    # single column
    dataframe = pd.DataFrame({"ColumnB": [2, 3, 4]})
    dataframe = sql.insert.insert(table_name, dataframe)
    result = conversion.read_values(
        f"SELECT ColumnB FROM {table_name}", schema, sql.connection
    )
    assert result["ColumnB"].equals(pd.Series([pd.NA, 2, 3, 4], dtype="Int32"))

    # single column of dates
    dataframe = pd.DataFrame(
        {"ColumnC": ["06-22-2021", "06-22-2021"]}, dtype="datetime64[ns]"
    )
    dataframe = sql.insert.insert(table_name, dataframe)
    result = conversion.read_values(
        f"SELECT ColumnC FROM {table_name}", schema, sql.connection
    )
    assert result["ColumnC"].equals(
        pd.Series(
            [pd.NA, pd.NA, pd.NA, pd.NA, "06-22-2021", "06-22-2021"],
            dtype="datetime64[ns]",
        )
    )


def test_insert_composite_pk(sql):
    table_name = "##test_insert_composite_pk"

    columns = columns = {
        "ColumnA": "TINYINT",
        "ColumnB": "VARCHAR(5)",
        "ColumnC": "BIGINT",
    }
    sql.create.table(table_name, columns, primary_key_column=["ColumnA", "ColumnB"])

    dataframe = pd.DataFrame({"ColumnA": [1], "ColumnB": ["12345"], "ColumnC": [1]})
    dataframe = sql.insert.insert(table_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert all(result.index == pd.MultiIndex.from_tuples([(1, "12345")]))
    assert all(result["ColumnC"] == 1)


def test_insert_include_metadata_timestamps(sql, caplog):
    table_name = "##test_insert_include_metadata_timestamps"

    # sample data
    dataframe = pd.DataFrame({"_bit": pd.Series([1, 0, None], dtype="boolean")})

    # create table
    sql.create.table(table_name, columns={"_bit": "BIT"})

    # insert data
    dataframe = sql.insert_meta.insert(table_name, dataframe)

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert all(result["_time_insert"].notna())
    assert result["_bit"].equals(dataframe["_bit"])

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert (
        caplog.record_tuples[0][2]
        == f"Creating column '_time_insert' in table '{table_name}' with data type 'datetime2'."
    )
