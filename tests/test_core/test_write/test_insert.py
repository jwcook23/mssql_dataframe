import warnings

import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_warnings, custom_errors, create, conversion
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
    db = connect(database="tempdb", server="localhost")
    yield package(db)
    db.connection.close()


def test_insert_errors(sql):

    table_name = "##test_errors"

    sql.create.table(
        table_name, columns={"ColumnA": "SMALLINT", "ColumnB": "VARCHAR(1)"}
    )

    with pytest.raises(custom_errors.SQLTableDoesNotExist):
        dataframe = pd.DataFrame({"ColumnA": [1]})
        sql.insert.insert("error" + table_name, dataframe=dataframe)

    with pytest.raises(custom_errors.SQLColumnDoesNotExist):
        dataframe = pd.DataFrame({"ColumnC": [1]})
        sql.insert.insert(table_name, dataframe=dataframe)

    with pytest.raises(custom_errors.SQLInsufficientColumnSize):
        dataframe = pd.DataFrame({"ColumnB": ["aaa"]})
        sql.insert.insert(table_name, dataframe=dataframe)

    with pytest.raises(custom_errors.SQLInsufficientColumnSize):
        sql.insert.insert(table_name, dataframe=pd.DataFrame({"ColumnA": [100000]}))

    with pytest.raises(RecursionError):
        sql.insert._adjust_sql_attempts = 0
        sql.insert._target_table(
            table_name="##non_existant",
            dataframe=pd.DataFrame({"ColumnA": [100000]}),
            cursor=sql.insert._connection.cursor(),
        )


def test_insert_dataframe(sql):

    table_name = "##test_insert_dataframe"

    # sample data
    dataframe = pd.DataFrame(
        {
            "_bit": pd.Series([1, 0, None], dtype="boolean"),
            "_tinyint": pd.Series([0, 255, None], dtype="UInt8"),
            "_smallint": pd.Series([-(2 ** 15), 2 ** 15 - 1, None], dtype="Int16"),
            "_int": pd.Series([-(2 ** 31), 2 ** 31 - 1, None], dtype="Int32"),
            "_bigint": pd.Series([-(2 ** 63), 2 ** 63 - 1, None], dtype="Int64"),
            "_float": pd.Series([-(1.79 ** 308), 1.79 ** 308, None], dtype="float"),
            "_time": pd.Series(
                ["00:00:00.0000000", "23:59:59.9999999", None], dtype="timedelta64[ns]"
            ),
            "_date": pd.Series(
                [
                    (pd.Timestamp.min + pd.DateOffset(days=1)).date(),
                    pd.Timestamp.max.date(),
                    None,
                ],
                dtype="datetime64[ns]",
            ),
            "_datetime2": pd.Series(
                [pd.Timestamp.min, pd.Timestamp.max, None], dtype="datetime64[ns]"
            ),
            "_varchar": pd.Series(["a", "bbb", None], dtype="string"),
            "_nvarchar": pd.Series(
                ["100\N{DEGREE SIGN}F", "company name\N{REGISTERED SIGN}", None],
                dtype="string",
            ),
        }
    )

    # create table
    columns = {
        "_time_insert": "DATETIME2",
        "_bit": "BIT",
        "_tinyint": "TINYINT",
        "_smallint": "SMALLINT",
        "_int": "INT",
        "_bigint": "BIGINT",
        "_float": "FLOAT",
        "_time": "TIME",
        "_date": "DATE",
        "_datetime2": "DATETIME2",
        "_varchar": "VARCHAR",
        "_nvarchar": "NVARCHAR",
    }
    columns["_varchar"] = (
        columns["_varchar"] + "(" + str(dataframe["_varchar"].str.len().max()) + ")"
    )
    columns["_nvarchar"] = (
        columns["_nvarchar"] + "(" + str(dataframe["_nvarchar"].str.len().max()) + ")"
    )
    sql.create.table(table_name, columns)

    # insert data
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.insert_meta.insert(table_name, dataframe)
        assert len(warn) == 1
        assert isinstance(
            warn[0].message, custom_warnings.SQLDataTypeDATETIME2Truncation
        )
        assert (
            str(warn[0].message)
            == "Nanosecond precision for dataframe columns ['_datetime2'] will be truncated as SQL data type DATETIME2 allows 7 max decimal places."
        )

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert all(result["_time_insert"].notna())
    assert dataframe.equals(result[result.columns.drop("_time_insert")])


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


def test_insert_include_metadata_timestamps(sql):

    table_name = "##test_insert_include_metadata_timestamps"

    # sample data
    dataframe = pd.DataFrame({"_bit": pd.Series([1, 0, None], dtype="boolean")})

    # create table
    sql.create.table(table_name, columns={"_bit": "BIT"})

    # insert data
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.insert_meta.insert(table_name, dataframe)
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert (
            str(warn[0].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert all(result["_time_insert"].notna())
    assert result["_bit"].equals(dataframe["_bit"])
