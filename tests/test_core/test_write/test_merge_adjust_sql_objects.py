import warnings

import pytest
import pandas as pd

pd.options.mode.chained_assignment = "raise"

from mssql_dataframe import connect
from mssql_dataframe.core import custom_warnings, create, conversion
from mssql_dataframe.core.write import merge


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(connection)
        self.merge = merge.merge(connection, autoadjust_sql_objects=True)
        self.merge_meta = merge.merge(connection, include_metadata_timestamps=True, autoadjust_sql_objects=True)

@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name="tempdb", server_name="localhost")
    yield package(db)
    db.connection.close()


def test_merge_create_table(sql):

    table_name = "##test_merge_create_table"
    dataframe = pd.DataFrame(
        {"_pk": [1, 2], "ColumnA": [5, 6], "ColumnB": ["06/22/2021", "2023-08-31"]}
    )

    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(
            table_name, dataframe, match_columns=["_pk"]
        )
        assert len(warn) == 4
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert str(warn[0].message) == f"Creating table {table_name}"
        assert f"Created table: {table_name}" in str(warn[1].message)
        assert (
            str(warn[2].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[3].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].isna())
    assert all(result["_time_insert"].notna())


def test_merge_add_column(sql):

    table_name = "##test_merge_add_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2]})
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index != 0]
    dataframe["NewColumn"] = [3]
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(table_name, dataframe)
        assert len(warn) == 3
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[2].message)
            == f"Creating column NewColumn in table {table_name} with data type tinyint."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna())
    assert all(result["_time_insert"].isna())


def test_merge_alter_column(sql):

    table_name = "##test_merge_alter_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2], "ColumnB": ["a", "b"]})
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index != 0]
    dataframe["ColumnA"] = dataframe["ColumnA"].astype("Int64")
    dataframe.loc[1, "ColumnA"] = 10000
    dataframe.loc[1, "ColumnB"] = "bbbbb"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(table_name, dataframe)
        assert len(warn) == 4
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[2].message)
            == f"Altering column ColumnA in table {table_name} to data type bigint with is_nullable=False."
        )
        assert (
            str(warn[3].message)
            == f"Altering column ColumnB in table {table_name} to data type varchar(5) with is_nullable=False."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna())
    assert all(result["_time_insert"].isna())


def test_merge_add_and_alter_column(sql):

    table_name = "##test_merge_add_and_alter_column"
    dataframe = pd.DataFrame({"ColumnA": [1, 2], "ColumnB": ["a", "b"]})
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key="index")
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # merge using the SQL primary key that came from the dataframe's index
    dataframe = dataframe[dataframe.index != 0]
    dataframe.loc[1, "ColumnA"] = 3
    dataframe.loc[1, "ColumnB"] = "bbbbb"
    dataframe["NewColumn"] = 0
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(table_name, dataframe)
        assert len(warn) == 4
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[2].message)
            == f"Creating column NewColumn in table {table_name} with data type tinyint."
        )
        assert (
            str(warn[3].message)
            == f"Altering column ColumnB in table {table_name} to data type varchar(5) with is_nullable=False."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna())
    assert all(result["_time_insert"].isna())
