import env
import logging

import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create, conversion
from mssql_dataframe.core.write import merge

pd.options.mode.chained_assignment = "raise"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.merge = merge.merge(self.connection, autoadjust_sql_objects=True)
        self.merge_meta = merge.merge(
            self.connection,
            include_metadata_timestamps=True,
            autoadjust_sql_objects=True,
        )


@pytest.fixture(scope="module")
def sql():
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    yield package(db)
    db.connection.close()


def test_merge_create_table(sql, caplog):

    table_name = "##test_merge_create_table"
    dataframe = pd.DataFrame(
        {"_pk": [1, 2], "ColumnA": [5, 6], "ColumnB": ["06/22/2021", "2023-08-31"]}
    )
    dataframe = sql.merge_meta.merge(table_name, dataframe, match_columns=["_pk"])

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].isna())
    assert all(result["_time_insert"].notna())

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 4
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert caplog.record_tuples[0][2] == f"Creating table '{table_name}'."
    assert caplog.record_tuples[1][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[1][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[1][2]
    assert caplog.record_tuples[2][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[2][1] == logging.WARNING
    assert (
        caplog.record_tuples[2][2]
        == f"Creating column '_time_update' in table '{table_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[3][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[3][1] == logging.WARNING
    assert (
        caplog.record_tuples[3][2]
        == f"Creating column '_time_insert' in table '{table_name}' with data type 'datetime2'."
    )


def test_merge_add_column(sql, caplog):

    table_name = "##test_merge_add_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2]})
    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="index"
    )

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index != 0]
    dataframe["NewColumn"] = [3]
    dataframe = sql.merge_meta.merge(table_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna())
    assert all(result["_time_insert"].isna())

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 4
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert f"Created table: {table_name}" in caplog.record_tuples[0][2]
    assert caplog.record_tuples[1][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[1][1] == logging.WARNING
    assert (
        caplog.record_tuples[1][2]
        == f"Creating column '_time_update' in table '{table_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[2][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[2][1] == logging.WARNING
    assert (
        caplog.record_tuples[2][2]
        == f"Creating column '_time_insert' in table '{table_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[3][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[3][1] == logging.WARNING
    assert (
        caplog.record_tuples[3][2]
        == f"Creating column 'NewColumn' in table '{table_name}' with data type 'tinyint'."
    )


def test_merge_alter_column(sql, caplog):

    table_name = "##test_merge_alter_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2], "ColumnB": ["a", "b"]})
    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="index"
    )

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index != 0]
    dataframe["ColumnA"] = dataframe["ColumnA"].astype("Int64")
    dataframe.loc[1, "ColumnA"] = 10000
    dataframe.loc[1, "ColumnB"] = "bbbbb"
    dataframe = sql.merge_meta.merge(table_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna())
    assert all(result["_time_insert"].isna())

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 5
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert f"Created table: {table_name}" in caplog.record_tuples[0][2]
    assert caplog.record_tuples[1][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[1][1] == logging.WARNING
    assert (
        caplog.record_tuples[1][2]
        == f"Creating column '_time_update' in table '{table_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[2][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[2][1] == logging.WARNING
    assert (
        caplog.record_tuples[2][2]
        == f"Creating column '_time_insert' in table '{table_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[3][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[3][1] == logging.WARNING
    assert (
        caplog.record_tuples[3][2]
        == f"Altering column 'ColumnA' in table '{table_name}' to data type 'smallint' with 'is_nullable=False'."
    )
    assert caplog.record_tuples[4][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[4][1] == logging.WARNING
    assert (
        caplog.record_tuples[4][2]
        == f"Altering column 'ColumnB' in table '{table_name}' to data type 'varchar(5)' with 'is_nullable=False'."
    )


def test_merge_add_and_alter_column(sql, caplog):

    table_name = "##test_merge_add_and_alter_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2], "ColumnB": ["a", "b"]})
    sql.create.table_from_dataframe(table_name, dataframe, primary_key="index")

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index != 0]
    dataframe.loc[1, "ColumnA"] = 3
    dataframe.loc[1, "ColumnB"] = "bbbbb"
    dataframe["NewColumn"] = 0
    dataframe = sql.merge_meta.merge(table_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna())
    assert all(result["_time_insert"].isna())

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 5
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert f"Created table: {table_name}" in caplog.record_tuples[0][2]
    assert caplog.record_tuples[1][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[1][1] == logging.WARNING
    assert (
        caplog.record_tuples[1][2]
        == f"Creating column '_time_update' in table '{table_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[2][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[2][1] == logging.WARNING
    assert (
        caplog.record_tuples[2][2]
        == f"Creating column '_time_insert' in table '{table_name}' with data type 'datetime2'."
    )
    assert caplog.record_tuples[3][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[3][1] == logging.WARNING
    assert (
        caplog.record_tuples[3][2]
        == f"Creating column 'NewColumn' in table '{table_name}' with data type 'tinyint'."
    )
    assert caplog.record_tuples[4][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[4][1] == logging.WARNING
    assert (
        caplog.record_tuples[4][2]
        == f"Altering column 'ColumnB' in table '{table_name}' to data type 'varchar(5)' with 'is_nullable=False'."
    )
