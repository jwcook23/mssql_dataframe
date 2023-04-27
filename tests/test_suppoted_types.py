import env
import logging
from decimal import Decimal

import pandas as pd
from numpy import inf
import pytest

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create
from mssql_dataframe.core import conversion_rules
from mssql_dataframe.core.write import insert, update, merge
from mssql_dataframe.core.read import read
from mssql_dataframe.__equality__ import compare_dfs


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.read = read(self.connection)
        self.insert = insert.insert(self.connection)
        self.update = update.update(self.connection)
        self.merge = merge.merge(self.connection)


@pytest.fixture(scope="module")
def sql():
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    yield package(db)
    db.connection.close()


@pytest.fixture
def sample():

    # data types to test with the column name as the data type prepended by an underscore
    # Index=1: truncation test 2 if applicable or another value
    # Index=2: truncation test 2 if applicable or another value
    # Index=3: null value
    dataframe = pd.DataFrame(
        {
            "_bit": pd.Series([1, 0, None], dtype="boolean"),
            "_tinyint": pd.Series([0, 255, None], dtype="UInt8"),
            "_smallint": pd.Series([-(2**15), 2**15 - 1, None], dtype="Int16"),
            "_int": pd.Series([-(2**31), 2**31 - 1, None], dtype="Int32"),
            "_bigint": pd.Series([-(2**63), 2**63 - 1, None], dtype="Int64"),
            "_float": pd.Series([-(1.79**308), 1.79**308, None], dtype="float"),
            "_numeric": pd.Series(
                [Decimal("1.23"), Decimal("4.56789"), None], dtype="object"
            ),
            "_decimal": pd.Series(
                [Decimal("11.23"), Decimal("44.56789"), None], dtype="object"
            ),
            "_time": pd.Series(
                ["00:00:00.000000123", "23:59:59.123456789", None],
                dtype="timedelta64[ns]",
            ),
            "_date": pd.Series(
                [
                    (pd.Timestamp.min + pd.Timedelta(days=1)).date(),
                    pd.Timestamp.max.date(),
                    None,
                ],
                dtype="datetime64[ns]",
            ),
            "_datetime": pd.Series(
                ["1900-01-01 00:00:00.008", "1900-01-01 00:00:00.009", None],
                dtype="datetime64[ns]",
            ),
            "_datetimeoffset": pd.Series(
                [
                    "1900-01-01 00:00:00.123456789+10:30",
                    "1900-01-01 00:00:00.12-9:15",
                    None,
                ],
                dtype="object",
            ),
            "_datetime2": pd.Series(
                [pd.Timestamp.min, pd.Timestamp.max, None], dtype="datetime64[ns]"
            ),
            "_char": pd.Series(["a", "b", None], dtype="string"),
            "_nchar": pd.Series(["い", "え", None], dtype="string"),
            "_varchar": pd.Series(["a", "bbb", None], dtype="string"),
            "_nvarchar": pd.Series(["い", "いえ", None], dtype="string"),
        }
    )

    # add min and max values from conversion_rules
    # Index=4: min value
    # Index=5: max value
    extremes = conversion_rules.rules[["sql_type", "min_value", "max_value"]].copy()
    extremes["sql_type"] = "_" + extremes["sql_type"]
    extremes = extremes.T
    extremes.columns = extremes.loc["sql_type"]
    extremes = extremes.drop("sql_type")
    extremes.dtypes
    extremes = extremes.astype(dataframe.dtypes)
    extremes = extremes.replace([-inf, inf], pd.NA)
    dataframe = pd.concat([dataframe, extremes], ignore_index=True)

    columns = {
        "_bit": "BIT",
        "_tinyint": "TINYINT",
        "_smallint": "SMALLINT",
        "_int": "INT",
        "_bigint": "BIGINT",
        "_float": "FLOAT",
        "_numeric": "NUMERIC(5,2)",
        "_decimal": "DECIMAL(8,6)",
        "_time": "TIME",
        "_date": "DATE",
        "_datetime": "DATETIME",
        "_datetimeoffset": "DATETIMEOFFSET",
        "_datetime2": "DATETIME2",
        "_char": "CHAR(1)",
        "_nchar": "NCHAR(1)",
        "_varchar": "VARCHAR(3)",
        "_nvarchar": "NVARCHAR(2)",
    }

    return {"dataframe": dataframe, "columns": columns}


def check_expected_warnings(caplog):

    assert len(caplog.record_tuples) == 5

    for log_num in caplog.record_tuples:
        assert log_num[0] == "mssql_dataframe.core.conversion"
        assert log_num[1] == logging.WARNING

    assert (
        caplog.record_tuples[0][2]
        == "Nanosecond precision for dataframe columns ['_time'] will be rounded as SQL data type 'time' allows 7 max decimal places."
    )

    assert (
        caplog.record_tuples[1][2]
        == "Millisecond precision for dataframe columns ['_datetime'] will be rounded as SQL data type 'datetime' rounds to increments of .000, .003, or .007 seconds."
    )

    assert (
        caplog.record_tuples[2][2]
        == "Nanosecond precision for dataframe columns ['_datetime2'] will be rounded as SQL data type 'datetime2' allows 7 max decimal places."
    )

    assert (
        caplog.record_tuples[3][2]
        == "Nanosecond precision for dataframe columns ['_datetimeoffset'] will be rounded as SQL data type 'datetimeoffset' allows 7 max decimal places."
    )

    assert (
        caplog.record_tuples[4][2]
        == "Decimal digits for column [_numeric] will be rounded to 2 decimal places to fit SQL specification for this column."
    )

    caplog.clear()

    return caplog


def test_insert(sql, sample, caplog):

    table_name = "##test_supported_dtypes_insert"

    sql.create.table(table_name, sample["columns"])

    df = sql.insert.insert(table_name, sample["dataframe"])
    caplog = check_expected_warnings(caplog)

    result = sql.read.table(table_name)
    compare_dfs(df, result)


def test_update(sql, sample, caplog):

    table_name = "##test_supported_dtypes_update"

    # create table with primary key for updating
    columns = sample["columns"]
    columns["pk"] = "TINYINT"
    sql.create.table(table_name, columns, primary_key_column="pk")

    # add primary key to dataframe for updating then insert
    base = sample["dataframe"].copy()
    base.index.name = "pk"
    _ = sql.insert.insert(table_name, base)
    caplog = check_expected_warnings(caplog)

    # update with the same data to insure all datatypes can be inserted into source table
    df = sql.merge.merge(table_name, base)
    caplog = check_expected_warnings(caplog)

    # validate results in SQL against dataframe
    result = sql.read.table(table_name)
    compare_dfs(df, result)


def test_merge(sql, sample, caplog):

    table_name = "##test_supported_dtypes_merge"

    # create table with primary key for merging
    columns = sample["columns"]
    columns["pk"] = "TINYINT"
    sql.create.table(table_name, columns, primary_key_column="pk")

    # add primary key to dataframe for merging then insert
    base = sample["dataframe"].copy()
    base.index.name = "pk"
    _ = sql.insert.insert(table_name, base)
    caplog = check_expected_warnings(caplog)

    # merge with the same data to insure all datatypes can be inserted into source table
    df = sql.update.update(table_name, base)
    caplog = check_expected_warnings(caplog)

    # validate results in SQL against dataframe
    result = sql.read.table(table_name)
    compare_dfs(df, result)
