import env
import logging

import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_errors, create, conversion
from mssql_dataframe.core.write import update

pd.options.mode.chained_assignment = "raise"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.update = update.update(self.connection, autoadjust_sql_objects=True)
        self.update_meta = update.update(
            self.connection,
            include_metadata_timestamps=True,
            autoadjust_sql_objects=True,
        )


@pytest.fixture(scope="module")
def sql():
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    yield package(db)
    db.connection.close()


def test_update_create_table(sql):
    """Updating a table that doesn't exist should always raise an error, even
    if autoadjust_sql_objects=True."""

    table_name = "##test_update_create_table"

    dataframe = pd.DataFrame({"_pk": [0, 1], "ColumnA": [1, 2]}).set_index(keys="_pk")

    with pytest.raises(custom_errors.SQLTableDoesNotExist):
        sql.update.update(table_name, dataframe)


def test_update_add_column(sql, caplog):

    table_name = "##test_update_add_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2]})
    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="index"
    )

    # update using the SQL primary key that came from the dataframe's index
    dataframe["NewColumn"] = [3, 4]
    updated = sql.update_meta.update(table_name, dataframe[["NewColumn"]])
    dataframe["NewColumn"] = updated["NewColumn"]

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert result["_time_update"].notna().all()

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 3
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
        == f"Creating column 'NewColumn' in table '{table_name}' with data type 'tinyint'."
    )


def test_update_alter_column(sql, caplog):

    table_name = "##test_update_alter_column"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [0, 0]}
    )
    sql.create.table_from_dataframe(table_name, dataframe, primary_key=None)

    # update using ColumnA
    dataframe["ColumnB"] = ["aaa", "bbb"]
    dataframe["ColumnC"] = [256, 256]
    updated = sql.update_meta.update(table_name, dataframe, match_columns=["ColumnA"])
    dataframe[["ColumnB", "ColumnC"]] = updated[["ColumnB", "ColumnC"]]

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert result["_time_update"].notna().all()

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
        == f"Altering column 'ColumnB' in table '{table_name}' to data type 'char(3)' with 'is_nullable=False'."
    )
    assert caplog.record_tuples[3][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[3][1] == logging.WARNING
    assert (
        caplog.record_tuples[3][2]
        == f"Altering column 'ColumnC' in table '{table_name}' to data type 'smallint' with 'is_nullable=False'."
    )


def test_update_add_and_alter_column(sql, caplog):

    table_name = "##test_update_add_and_alter_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2], "ColumnB": ["a", "b"]})
    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="index"
    )

    # update using the SQL primary key that came from the dataframe's index
    dataframe["ColumnB"] = ["aaa", "bbb"]
    dataframe["NewColumn"] = [3, 4]
    updated = sql.update_meta.update(table_name, dataframe[["ColumnB", "NewColumn"]])
    dataframe[["ColumnB", "NewColumn"]] = updated[["ColumnB", "NewColumn"]]

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert result["_time_update"].notna().all()

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
        == f"Creating column 'NewColumn' in table '{table_name}' with data type 'tinyint'."
    )
    assert caplog.record_tuples[3][0] == "mssql_dataframe.core.write._exceptions"
    assert caplog.record_tuples[3][1] == logging.WARNING
    assert (
        caplog.record_tuples[3][2]
        == f"Altering column 'ColumnB' in table '{table_name}' to data type 'char(3)' with 'is_nullable=False'."
    )
