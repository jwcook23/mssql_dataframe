import warnings

import pytest
import pandas as pd

pd.options.mode.chained_assignment = "raise"

from mssql_dataframe import connect
from mssql_dataframe.core import custom_warnings, custom_errors, create, conversion
from mssql_dataframe.core.write import update

from mssql_dataframe.core.write import update


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(connection)
        self.update = update.update(connection, autoadjust_sql_objects=True)
        self.update_meta = update.update(connection, include_metadata_timestamps=True, autoadjust_sql_objects=True)


@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name="tempdb", server_name="localhost")
    yield package(db)
    db.connection.close()


def test_update_create_table(sql):
    """Updating a table that doesn't exist should always raise an error, even
    if autoadjust_sql_objects=True."""

    table_name = "##test_update_create_table"

    dataframe = pd.DataFrame({"_pk": [0, 1], "ColumnA": [1, 2]}).set_index(keys="_pk")

    with pytest.raises(custom_errors.SQLTableDoesNotExist):
        sql.update.update(table_name, dataframe)


def test_update_add_column(sql):

    table_name = "##test_update_add_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2]})
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key="index")
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    dataframe, _ = sql.update.insert(table_name, dataframe)

    # update using the SQL primary key that came from the dataframe's index
    dataframe["NewColumn"] = [3, 4]
    with warnings.catch_warnings(record=True) as warn:
        updated, schema = sql.update_meta.update(table_name, dataframe[["NewColumn"]])
        dataframe["NewColumn"] = updated["NewColumn"]
        assert len(warn) == 2
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column NewColumn in table {table_name} with data type tinyint."
        )

        result = conversion.read_values(
            f"SELECT * FROM {table_name}", schema, sql.connection
        )
        assert result[dataframe.columns].equals(dataframe)
        assert result["_time_update"].notna().all()


def test_update_alter_column(sql):

    table_name = "##test_update_alter_column"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [0, 0]}
    )
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key=None)
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    dataframe, schema = sql.update.insert(
        table_name, dataframe
    )

    # update using ColumnA
    dataframe["ColumnB"] = ["aaa", "bbb"]
    dataframe["ColumnC"] = [256, 256]
    with warnings.catch_warnings(record=True) as warn:
        updated, schema = sql.update_meta.update(
            table_name, dataframe, match_columns=["ColumnA"]
        )
        dataframe[["ColumnB", "ColumnC"]] = updated[["ColumnB", "ColumnC"]]
        assert len(warn) == 3
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Altering column ColumnB in table {table_name} to data type varchar(3) with is_nullable=False."
        )
        assert (
            str(warn[2].message)
            == f"Altering column ColumnC in table {table_name} to data type smallint with is_nullable=False."
        )

        result = conversion.read_values(
            f"SELECT * FROM {table_name}", schema, sql.connection
        )
        assert result[dataframe.columns].equals(dataframe)
        assert result["_time_update"].notna().all()


def test_update_add_and_alter_column(sql):

    table_name = "##test_update_add_and_alter_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2], "ColumnB": ["a", "b"]})
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key="index")
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    dataframe, schema = sql.update.insert(
        table_name, dataframe
    )

    # update using the SQL primary key that came from the dataframe's index
    dataframe["ColumnB"] = ["aaa", "bbb"]
    dataframe["NewColumn"] = [3, 4]
    with warnings.catch_warnings(record=True) as warn:
        updated, schema = sql.update_meta.update(
            table_name, dataframe[["ColumnB", "NewColumn"]]
        )
        dataframe[["ColumnB", "NewColumn"]] = updated[["ColumnB", "NewColumn"]]
        assert len(warn) == 3
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column NewColumn in table {table_name} with data type tinyint."
        )
        assert (
            str(warn[2].message)
            == f"Altering column ColumnB in table {table_name} to data type varchar(3) with is_nullable=False."
        )

        result = conversion.read_values(
            f"SELECT * FROM {table_name}", schema, sql.connection
        )
        assert result[dataframe.columns].equals(dataframe)
        assert result["_time_update"].notna().all()
