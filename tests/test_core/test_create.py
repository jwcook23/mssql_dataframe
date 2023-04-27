import env

from datetime import datetime

import pytest
import pandas as pd
import pyodbc

from mssql_dataframe.connect import connect
from mssql_dataframe.core import conversion, create

pd.options.mode.chained_assignment = "raise"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.create_meta = create.create(
            self.connection, include_metadata_timestamps=True
        )


@pytest.fixture(scope="module")
def sql():
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    yield package(db)
    db.connection.close()


@pytest.fixture(scope="module")
def sample():
    dataframe = pd.DataFrame(
        {
            "_char": [None, "b", "c", "4", "e"],
            "_tinyint": [None, 2, 3, 4, 5],
            "_smallint": [256, 2, 6, 4, 5],  # tinyint max is 255
            "_int": [32768, 2, 3, 4, 5],  # smallint max is 32,767
            "_bigint": [2147483648, 2, 3, None, 5],  # int max size is 2,147,483,647
            "_float": [1.111111, 2, 3, 4, 5],  # any decicmal places
            "_time": [str(datetime.now().time())]
            * 5,  # string in format HH:MM:SS.ffffff
            "_datetime": [datetime.now()] * 4 + [pd.NaT],
            "_empty": [None] * 5,
        }
    )
    return dataframe


@pytest.fixture(scope="module")
def validation():

    expected = pd.DataFrame.from_records(
        [
            {
                "column_name": "_index",
                "sql_type": "tinyint",
                "is_nullable": False,
                "ss_is_identity": False,
                "pk_seq": 1,
                "pandas_type": "UInt8",
                "odbc_type": pyodbc.SQL_TINYINT,
                "odbc_size": 1,
                "odbc_precision": 0,
            },
            {
                "column_name": "_pk",
                "sql_type": "int identity",
                "is_nullable": False,
                "ss_is_identity": True,
                "pk_seq": 1,
                "pandas_type": "Int32",
                "odbc_type": pyodbc.SQL_INTEGER,
                "odbc_size": 4,
                "odbc_precision": 0,
            },
            {
                "column_name": "_char",
                "sql_type": "char",
                "is_nullable": True,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "string",
                "odbc_type": pyodbc.SQL_CHAR,
                "odbc_size": 0,
                "odbc_precision": 0,
            },
            {
                "column_name": "_tinyint",
                "sql_type": "tinyint",
                "is_nullable": True,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "UInt8",
                "odbc_type": pyodbc.SQL_TINYINT,
                "odbc_size": 1,
                "odbc_precision": 0,
            },
            {
                "column_name": "_smallint",
                "sql_type": "smallint",
                "is_nullable": False,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "Int16",
                "odbc_type": pyodbc.SQL_SMALLINT,
                "odbc_size": 2,
                "odbc_precision": 0,
            },
            {
                "column_name": "_int",
                "sql_type": "int",
                "is_nullable": False,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "Int32",
                "odbc_type": pyodbc.SQL_INTEGER,
                "odbc_size": 4,
                "odbc_precision": 0,
            },
            {
                "column_name": "_bigint",
                "sql_type": "bigint",
                "is_nullable": True,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "Int64",
                "odbc_type": pyodbc.SQL_BIGINT,
                "odbc_size": 8,
                "odbc_precision": 0,
            },
            {
                "column_name": "_float",
                "sql_type": "float",
                "is_nullable": False,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "float64",
                "odbc_type": pyodbc.SQL_FLOAT,
                "odbc_size": 8,
                "odbc_precision": 53,
            },
            {
                "column_name": "_time",
                "sql_type": "time",
                "is_nullable": False,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "timedelta64[ns]",
                "odbc_type": pyodbc.SQL_SS_TIME2,
                "odbc_size": 16,
                "odbc_precision": 7,
            },
            {
                "column_name": "_datetime",
                "sql_type": "datetime2",
                "is_nullable": True,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "datetime64[ns]",
                "odbc_type": pyodbc.SQL_TYPE_TIMESTAMP,
                "odbc_size": 27,
                "odbc_precision": 7,
            },
            {
                "column_name": "_empty",
                "sql_type": "nvarchar",
                "is_nullable": True,
                "ss_is_identity": False,
                "pk_seq": pd.NA,
                "pandas_type": "string",
                "odbc_type": pyodbc.SQL_WVARCHAR,
                "odbc_size": 0,
                "odbc_precision": 0,
            },
        ]
    )

    columns = ["column_name", "sql_type", "pandas_type"]
    expected[columns] = expected[columns].astype("string")
    expected[["pk_seq"]] = expected[["pk_seq"]].astype("Int64")

    expected = expected.set_index("column_name")

    return expected


def test_table_errors(sql):

    table_name = "##test_table_column"

    with pytest.raises(KeyError):
        columns = {"A": "VARCHAR"}
        sql.create.table(table_name, columns, primary_key_column="Z")


def test_table_column(sql):

    table_name = "dbo.##test_table_column"
    columns = {"A": "VARCHAR"}
    sql.create.table(table_name, columns)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 1
    assert all(schema.index == "A")
    assert all(schema["sql_type"] == "varchar")
    assert all(schema["is_nullable"])
    assert all(~schema["ss_is_identity"])
    assert all(schema["pk_seq"].isna())
    assert all(schema["pk_name"].isna())
    assert all(schema["pandas_type"] == "string")
    assert all(schema["odbc_type"] == pyodbc.SQL_VARCHAR)


def test_table_pk(sql):

    table_name = "##test_table_pk"
    columns = {"A": "TINYINT", "B": "VARCHAR(100)", "C": "FLOAT"}
    primary_key_column = "A"
    not_nullable = "B"
    sql.create.table(
        table_name,
        columns,
        not_nullable=not_nullable,
        primary_key_column=primary_key_column,
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 3
    assert all(schema.index == ["A", "B", "C"])
    assert all(schema["sql_type"] == ["tinyint", "varchar", "float"])
    assert all(schema["is_nullable"] == [False, False, True])
    assert all(~schema["ss_is_identity"])
    assert schema["pk_seq"].equals(
        pd.Series(
            [1, pd.NA, pd.NA],
            index=pd.Series(["A", "B", "C"], dtype="string"),
            dtype="Int64",
        )
    )
    assert all(schema["pk_name"].isna() == [False, True, True])
    assert all(schema["pandas_type"] == ["UInt8", "string", "float64"])
    assert all(
        schema["odbc_type"]
        == [pyodbc.SQL_TINYINT, pyodbc.SQL_VARCHAR, pyodbc.SQL_FLOAT]
    )


def test_table_composite_pk(sql):

    table_name = "##test_table_composite_pk"
    columns = {"A": "TINYINT", "B": "VARCHAR(5)", "C": "FLOAT"}
    primary_key_column = ["A", "B"]
    not_nullable = "B"
    sql.create.table(
        table_name,
        columns,
        not_nullable=not_nullable,
        primary_key_column=primary_key_column,
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 3
    assert all(schema.index == ["A", "B", "C"])
    assert all(schema["sql_type"] == ["tinyint", "varchar", "float"])
    assert all(schema["is_nullable"] == [False, False, True])
    assert all(~schema["ss_is_identity"])
    assert schema["pk_seq"].equals(
        pd.Series(
            [1, 2, pd.NA],
            index=pd.Series(["A", "B", "C"], dtype="string"),
            dtype="Int64",
        )
    )
    assert all(schema["pk_name"].isna() == [False, False, True])
    assert all(schema["pandas_type"] == ["UInt8", "string", "float64"])
    assert all(
        schema["odbc_type"]
        == [pyodbc.SQL_TINYINT, pyodbc.SQL_VARCHAR, pyodbc.SQL_FLOAT]
    )


def test_table_pk_input_error(sql):

    with pytest.raises(ValueError):
        table_name = "##test_table_pk_input_error"
        columns = {"A": "TINYINT", "B": "VARCHAR(100)", "C": "DECIMAL(5,2)"}
        primary_key_column = "A"
        not_nullable = "B"
        sql.create.table(
            table_name,
            columns,
            not_nullable=not_nullable,
            primary_key_column=primary_key_column,
            sql_primary_key=True,
        )


def test_table_sqlpk(sql):

    table_name = "##test_table_sqlpk"
    columns = {"A": "VARCHAR"}
    sql.create.table(table_name, columns, sql_primary_key=True)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 2
    assert all(schema.index == ["_pk", "A"])
    assert all(schema["sql_type"] == ["int identity", "varchar"])
    assert all(schema["is_nullable"] == [False, True])
    assert all(schema["ss_is_identity"] == [True, False])
    assert schema["pk_seq"].equals(
        pd.Series(
            [1, pd.NA], index=pd.Series(["_pk", "A"], dtype="string"), dtype="Int64"
        )
    )
    assert all(schema["pk_name"].isna() == [False, True])
    assert all(schema["pandas_type"] == ["Int32", "string"])
    assert all(schema["odbc_type"] == [pyodbc.SQL_INTEGER, pyodbc.SQL_VARCHAR])
