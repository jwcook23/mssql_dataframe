from datetime import datetime
import warnings

import pytest
import pandas as pd

pd.options.mode.chained_assignment = "raise"
import pyodbc

from mssql_dataframe import connect
from mssql_dataframe.core import conversion, create, errors


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(connection)


@pytest.fixture(scope="module")
def sql():
    db = connect.connect(database_name="tempdb", server_name="localhost")
    yield package(db)
    db.connection.close()


@pytest.fixture(scope="module")
def sample():
    dataframe = pd.DataFrame(
        {
            "_varchar": [None, "b", "c", "4", "e"],
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


def test_table_errors(sql):

    table_name = "##test_table_column"

    with pytest.raises(KeyError):
        columns = {"A": "VARCHAR"}
        sql.create.table(table_name, columns, primary_key_column="Z")


def test_table_column(sql):

    table_name = "##test_table_column"
    columns = {"A": "VARCHAR"}
    sql.create.table(table_name, columns)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 1
    assert all(schema.index == "A")
    assert all(schema["sql_type"] == "varchar")
    assert all(schema["is_nullable"] == True)
    assert all(schema["ss_is_identity"] == False)
    assert all(schema["pk_seq"].isna())
    assert all(schema["pk_name"].isna())
    assert all(schema["pandas_type"] == "string")
    assert all(schema["odbc_type"] == pyodbc.SQL_VARCHAR)
    assert all(schema["odbc_size"] == 0)
    assert all(schema["odbc_precision"] == 0)


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
    assert all(schema["ss_is_identity"] == False)
    assert schema["pk_seq"].equals(
        pd.Series([1, pd.NA, pd.NA], index=["A", "B", "C"], dtype="Int64")
    )
    assert all(schema["pk_name"].isna() == [False, True, True])
    assert all(schema["pandas_type"] == ["UInt8", "string", "float64"])
    assert all(
        schema["odbc_type"]
        == [pyodbc.SQL_TINYINT, pyodbc.SQL_VARCHAR, pyodbc.SQL_FLOAT]
    )
    assert all(schema["odbc_size"] == [1, 0, 8])
    assert all(schema["odbc_precision"] == [0, 0, 53])


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
    assert all(schema["ss_is_identity"] == False)
    assert schema["pk_seq"].equals(
        pd.Series([1, 2, pd.NA], index=["A", "B", "C"], dtype="Int64")
    )
    assert all(schema["pk_name"].isna() == [False, False, True])
    assert all(schema["pandas_type"] == ["UInt8", "string", "float64"])
    assert all(
        schema["odbc_type"]
        == [pyodbc.SQL_TINYINT, pyodbc.SQL_VARCHAR, pyodbc.SQL_FLOAT]
    )
    assert all(schema["odbc_size"] == [1, 0, 8])
    assert all(schema["odbc_precision"] == [0, 0, 53])


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
        pd.Series([1, pd.NA], index=["_pk", "A"], dtype="Int64")
    )
    assert all(schema["pk_name"].isna() == [False, True])
    assert all(schema["pandas_type"] == ["Int32", "string"])
    assert all(schema["odbc_type"] == [pyodbc.SQL_INTEGER, pyodbc.SQL_VARCHAR])
    assert all(schema["odbc_size"] == [4, 0])
    assert all(schema["odbc_precision"] == [0, 0])


def test_table_from_dataframe_simple(sql):

    table_name = "##test_table_from_dataframe_simple"
    dataframe = pd.DataFrame({"ColumnA": [1]})
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(table_name, dataframe)
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 1
    assert all(schema.index == "ColumnA")
    assert all(schema["sql_type"] == "tinyint")
    assert all(schema["is_nullable"] == False)
    assert all(schema["ss_is_identity"] == False)
    assert all(schema["pk_seq"].isna())
    assert all(schema["pk_name"].isna())
    assert all(schema["pandas_type"] == "UInt8")
    assert all(schema["odbc_type"] == pyodbc.SQL_TINYINT)
    assert all(schema["odbc_size"] == 1)
    assert all(schema["odbc_precision"] == 0)


def test_table_from_dataframe_datestr(sql):
    table_name = "##test_table_from_dataframe_datestr"
    dataframe = pd.DataFrame({"ColumnA": ["06/22/2021"]})
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(table_name, dataframe)
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 1
    assert all(schema.index == "ColumnA")
    assert all(schema["sql_type"] == "date")
    assert all(schema["is_nullable"] == False)
    assert all(schema["ss_is_identity"] == False)
    assert all(schema["pk_seq"].isna())
    assert all(schema["pk_name"].isna())
    assert all(schema["pandas_type"] == "datetime64[ns]")
    assert all(schema["odbc_type"] == pyodbc.SQL_TYPE_DATE)
    assert all(schema["odbc_size"] == 10)
    assert all(schema["odbc_precision"] == 0)


def test_table_from_dataframe_errorpk(sql, sample):

    with pytest.raises(ValueError):
        table_name = "##test_table_from_dataframe_nopk"
        sql.create.table_from_dataframe(table_name, sample, primary_key="ColumnName")


def test_table_from_dataframe_nopk(sql, sample):

    table_name = "##test_table_from_dataframe_nopk"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, sample.copy(), primary_key=None
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    expected = pd.DataFrame(
        {
            "column_name": pd.Series(
                [
                    "_varchar",
                    "_tinyint",
                    "_smallint",
                    "_int",
                    "_bigint",
                    "_float",
                    "_time",
                    "_datetime",
                    "_empty",
                ],
                dtype="string",
            ),
            "sql_type": pd.Series(
                [
                    "varchar",
                    "tinyint",
                    "smallint",
                    "int",
                    "bigint",
                    "float",
                    "time",
                    "datetime2",
                    "nvarchar",
                ],
                dtype="string",
            ),
            "is_nullable": pd.Series(
                [True, True, False, False, True, False, False, True, True], dtype="bool"
            ),
            "ss_is_identity": pd.Series([False] * 9, dtype="bool"),
            "pk_seq": pd.Series([pd.NA] * 9, dtype="Int64"),
            "pk_name": pd.Series([pd.NA] * 9, dtype="string"),
            "pandas_type": pd.Series(
                [
                    "string",
                    "UInt8",
                    "Int16",
                    "Int32",
                    "Int64",
                    "float64",
                    "timedelta64[ns]",
                    "datetime64[ns]",
                    "string",
                ],
                dtype="string",
            ),
            "odbc_type": pd.Series(
                [
                    pyodbc.SQL_VARCHAR,
                    pyodbc.SQL_TINYINT,
                    pyodbc.SQL_SMALLINT,
                    pyodbc.SQL_INTEGER,
                    pyodbc.SQL_BIGINT,
                    pyodbc.SQL_FLOAT,
                    pyodbc.SQL_SS_TIME2,
                    pyodbc.SQL_TYPE_TIMESTAMP,
                    pyodbc.SQL_WVARCHAR,
                ],
                dtype="int64",
            ),
            "odbc_size": pd.Series([0, 1, 2, 4, 8, 8, 16, 27, 0], dtype="int64"),
            "odbc_precision": pd.Series([0, 0, 0, 0, 0, 53, 7, 7, 0], dtype="int64"),
        }
    ).set_index(keys="column_name")

    assert schema[expected.columns].equals(expected.loc[schema.index])


def test_table_from_dataframe_sqlpk(sql, sample):

    table_name = "##test_table_from_dataframe_sqlpk"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, sample.copy(), primary_key="sql"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    expected = pd.DataFrame(
        {
            "column_name": pd.Series(
                [
                    "_pk",
                    "_varchar",
                    "_tinyint",
                    "_smallint",
                    "_int",
                    "_bigint",
                    "_float",
                    "_time",
                    "_datetime",
                    "_empty",
                ],
                dtype="string",
            ),
            "sql_type": pd.Series(
                [
                    "int identity",
                    "varchar",
                    "tinyint",
                    "smallint",
                    "int",
                    "bigint",
                    "float",
                    "time",
                    "datetime2",
                    "nvarchar",
                ],
                dtype="string",
            ),
            "is_nullable": pd.Series(
                [False, True, True, False, False, True, False, False, True, True],
                dtype="bool",
            ),
            "ss_is_identity": pd.Series([True] + [False] * 9, dtype="bool"),
            "pk_seq": pd.Series([1] + [pd.NA] * 9, dtype="Int64"),
            "pandas_type": pd.Series(
                [
                    "Int32",
                    "string",
                    "UInt8",
                    "Int16",
                    "Int32",
                    "Int64",
                    "float64",
                    "timedelta64[ns]",
                    "datetime64[ns]",
                    "string",
                ],
                dtype="string",
            ),
            "odbc_type": pd.Series(
                [
                    pyodbc.SQL_INTEGER,
                    pyodbc.SQL_VARCHAR,
                    pyodbc.SQL_TINYINT,
                    pyodbc.SQL_SMALLINT,
                    pyodbc.SQL_INTEGER,
                    pyodbc.SQL_BIGINT,
                    pyodbc.SQL_FLOAT,
                    pyodbc.SQL_SS_TIME2,
                    pyodbc.SQL_TYPE_TIMESTAMP,
                    pyodbc.SQL_WVARCHAR,
                ],
                dtype="int64",
            ),
            "odbc_size": pd.Series([4, 0, 1, 2, 4, 8, 8, 16, 27, 0], dtype="int64"),
            "odbc_precision": pd.Series([0, 0, 0, 0, 0, 0, 53, 7, 7, 0], dtype="int64"),
        }
    ).set_index(keys="column_name")

    assert schema[expected.columns].equals(expected.loc[schema.index])
    assert pd.notna(schema.at["_pk", "pk_name"])
    assert schema.loc[schema.index != "_pk", "pk_name"].isna().all()


def test_table_from_dataframe_indexpk_unnamed(sql, sample):

    table_name = "##test_table_from_dataframe_indexpk_unnamed"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, sample.copy(), primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    expected = pd.DataFrame(
        {
            "column_name": pd.Series(
                [
                    "_index",
                    "_varchar",
                    "_tinyint",
                    "_smallint",
                    "_int",
                    "_bigint",
                    "_float",
                    "_time",
                    "_datetime",
                    "_empty",
                ],
                dtype="string",
            ),
            "sql_type": pd.Series(
                [
                    "tinyint",
                    "varchar",
                    "tinyint",
                    "smallint",
                    "int",
                    "bigint",
                    "float",
                    "time",
                    "datetime2",
                    "nvarchar",
                ],
                dtype="string",
            ),
            "is_nullable": pd.Series(
                [False, True, True, False, False, True, False, False, True, True],
                dtype="bool",
            ),
            "ss_is_identity": pd.Series([False] * 10, dtype="bool"),
            "pk_seq": pd.Series([1] + [pd.NA] * 9, dtype="Int64"),
            "pandas_type": pd.Series(
                [
                    "UInt8",
                    "string",
                    "UInt8",
                    "Int16",
                    "Int32",
                    "Int64",
                    "float64",
                    "timedelta64[ns]",
                    "datetime64[ns]",
                    "string",
                ],
                dtype="string",
            ),
            "odbc_type": pd.Series(
                [
                    pyodbc.SQL_TINYINT,
                    pyodbc.SQL_VARCHAR,
                    pyodbc.SQL_TINYINT,
                    pyodbc.SQL_SMALLINT,
                    pyodbc.SQL_INTEGER,
                    pyodbc.SQL_BIGINT,
                    pyodbc.SQL_FLOAT,
                    pyodbc.SQL_SS_TIME2,
                    pyodbc.SQL_TYPE_TIMESTAMP,
                    pyodbc.SQL_WVARCHAR,
                ],
                dtype="int64",
            ),
            "odbc_size": pd.Series([1, 0, 1, 2, 4, 8, 8, 16, 27, 0], dtype="int64"),
            "odbc_precision": pd.Series([0, 0, 0, 0, 0, 0, 53, 7, 7, 0], dtype="int64"),
        }
    ).set_index(keys="column_name")

    assert schema[expected.columns].equals(expected.loc[schema.index])
    assert pd.notna(schema.at["_index", "pk_name"])
    assert schema.loc[schema.index != "_index", "pk_name"].isna().all()


def test_table_from_dataframe_indexpk_named(sql, sample):

    table_name = "##test_table_from_dataframe_indexpk_named"
    sample.index.name = "NamedIndex"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, sample.copy(), primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    expected = pd.DataFrame(
        {
            "column_name": pd.Series(
                [
                    "NamedIndex",
                    "_varchar",
                    "_tinyint",
                    "_smallint",
                    "_int",
                    "_bigint",
                    "_float",
                    "_time",
                    "_datetime",
                    "_empty",
                ],
                dtype="string",
            ),
            "sql_type": pd.Series(
                [
                    "tinyint",
                    "varchar",
                    "tinyint",
                    "smallint",
                    "int",
                    "bigint",
                    "float",
                    "time",
                    "datetime2",
                    "nvarchar",
                ],
                dtype="string",
            ),
            "is_nullable": pd.Series(
                [False, True, True, False, False, True, False, False, True, True],
                dtype="bool",
            ),
            "ss_is_identity": pd.Series([False] * 10, dtype="bool"),
            "pk_seq": pd.Series([1] + [pd.NA] * 9, dtype="Int64"),
            "pandas_type": pd.Series(
                [
                    "UInt8",
                    "string",
                    "UInt8",
                    "Int16",
                    "Int32",
                    "Int64",
                    "float64",
                    "timedelta64[ns]",
                    "datetime64[ns]",
                    "string",
                ],
                dtype="string",
            ),
            "odbc_type": pd.Series(
                [
                    pyodbc.SQL_TINYINT,
                    pyodbc.SQL_VARCHAR,
                    pyodbc.SQL_TINYINT,
                    pyodbc.SQL_SMALLINT,
                    pyodbc.SQL_INTEGER,
                    pyodbc.SQL_BIGINT,
                    pyodbc.SQL_FLOAT,
                    pyodbc.SQL_SS_TIME2,
                    pyodbc.SQL_TYPE_TIMESTAMP,
                    pyodbc.SQL_WVARCHAR,
                ],
                dtype="int64",
            ),
            "odbc_size": pd.Series([1, 0, 1, 2, 4, 8, 8, 16, 27, 0], dtype="int64"),
            "odbc_precision": pd.Series([0, 0, 0, 0, 0, 0, 53, 7, 7, 0], dtype="int64"),
        }
    ).set_index(keys="column_name")

    assert schema[expected.columns].equals(expected.loc[schema.index])
    assert pd.notna(schema.at["NamedIndex", "pk_name"])
    assert schema.loc[schema.index != "NamedIndex", "pk_name"].isna().all()


def test_table_from_dataframe_inferpk_integer(sql):

    table_name = "##test_table_from_dataframe_inferpk_integer"
    dataframe = pd.DataFrame(
        {
            "_varchar1": ["a", "b", "c", "d", "e"],
            "_varchar2": ["aa", "b", "c", "d", "e"],
            "_tinyint": [None, 2, 3, 4, 5],
            "_smallint": [265, 2, 6, 4, 5],
            "_int": [32768, 2, 3, 4, 5],
            "_float1": [1.1111, 2, 3, 4, 5],
            "_float2": [1.1111, 2, 3, 4, 6],
        }
    )
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="infer"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert schema.at["_smallint", "pk_seq"] == 1
    assert all(schema.loc[schema.index != "_smallint", "pk_seq"].isna())


def test_table_from_dataframe_inferpk_string(sql):

    table_name = "##test_table_from_dataframe_inferpk_string"
    dataframe = pd.DataFrame(
        {
            "_varchar1": ["a", "b", "c", "d", "e"],
            "_varchar2": ["aa", "b", "c", "d", "e"],
        }
    )
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="infer"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert schema.at["_varchar1", "pk_seq"] == 1
    assert all(schema.loc[schema.index != "_varchar1", "pk_seq"].isna())


def test_table_from_dataframe_inferpk_none(sql):

    table_name = "##test_table_from_dataframe_inferpk_none"
    dataframe = pd.DataFrame(
        {
            "_varchar1": [None, "b", "c", "d", "e"],
            "_varchar2": [None, "b", "c", "d", "e"],
        }
    )

    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="infer"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert all(schema["pk_seq"].isna())


def test_table_from_dataframe_composite_pk(sql):

    table_name = "##test_table_from_dataframe_composite_pk"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]}
    )
    dataframe = dataframe.set_index(keys=["ColumnA", "ColumnB"])
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, errors.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)
    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert schema.at["ColumnA", "pk_seq"] == 1
    assert schema.at["ColumnB", "pk_seq"] == 2
