import env
import logging
from decimal import Decimal

import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create, conversion
from mssql_dataframe.core.write import insert
from mssql_dataframe.__equality__ import compare_dfs

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
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    yield package(db)
    db.connection.close()


def test_insert_dataframe(sql, caplog):

    table_name = "##test_insert_dataframe"

    # sample data
    dataframe = pd.DataFrame(
        {
            "_bit": pd.Series([1, 0, None], dtype="boolean"),
            "_tinyint": pd.Series([0, 255, None], dtype="UInt8"),
            "_smallint": pd.Series([-(2**15), 2**15 - 1, None], dtype="Int16"),
            "_int": pd.Series([-(2**31), 2**31 - 1, None], dtype="Int32"),
            "_bigint": pd.Series([-(2**63), 2**63 - 1, None], dtype="Int64"),
            "_float": pd.Series([-(1.79**308), 1.79**308, None], dtype="float"),
            "_numeric": pd.Series([Decimal('1.23'), Decimal('4.56789'), None], dtype="object"),
            "_decimal": pd.Series([Decimal('11.23'), Decimal('44.56789'), None], dtype="object"),
            "_time": pd.Series(
                ["00:00:00.0000000", "23:59:59.9999999", None], dtype="timedelta64[ns]"
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
                ['1900-01-01 00:00:00.003', '1900-01-01 00:00:00.008', '1900-01-01 00:00:00.009'], dtype="datetime64[ns]"
            ),
            "_datetimeoffset": pd.Series(
                ['1900-01-01 00:00:00.123456789+10:30', '1900-01-01 00:00:00.12-9:15', None], dtype="object"
            ),            
            "_datetime2": pd.Series(
                [pd.Timestamp.min, pd.Timestamp.max, None], dtype="datetime64[ns]"
            ),
            "_char": pd.Series([None, 'a', 'b'], dtype='string'),
            "_nchar": pd.Series([None, 'い', 'え'], dtype='string'),
            "_varchar": pd.Series(["a", "bbb", None], dtype="string"),
            "_nvarchar": pd.Series(['い','いえ', None], dtype="string"),
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
    sql.create.table(table_name, columns)

    # insert data
    dataframe = sql.insert_meta.insert(table_name, dataframe)

    # test result
    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert all(result["_time_insert"].notna())
    assert compare_dfs(dataframe, result[result.columns.drop("_time_insert")])

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 4
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.conversion"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert (
        caplog.record_tuples[0][2]
        == "Millisecond precision for dataframe columns ['_datetime'] will be rounded as SQL data type 'datetime' rounds to increments of .000, .003, or .007 seconds."
    )
    assert caplog.record_tuples[1][0] == "mssql_dataframe.core.conversion"
    assert caplog.record_tuples[1][1] == logging.WARNING
    assert (
        caplog.record_tuples[1][2]
        == "Nanosecond precision for dataframe columns ['_datetime2'] will be rounded as SQL data type 'datetime2' allows 7 max decimal places."
    )
    assert caplog.record_tuples[2][0] == "mssql_dataframe.core.conversion"
    assert caplog.record_tuples[2][1] == logging.WARNING
    assert (
        caplog.record_tuples[2][2]
        == "Nanosecond precision for dataframe columns ['_datetimeoffset'] will be rounded as SQL data type 'datetimeoffset' allows 7 max decimal places."
    )
    assert caplog.record_tuples[3][0] == "mssql_dataframe.core.conversion"
    assert caplog.record_tuples[3][1] == logging.WARNING
    assert (
        caplog.record_tuples[3][2]
        == "Decimal digits for column _numeric will be rounded to 2 decimal places to fit SQL data type 'numeric' specification."
    )    


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
