import env
import logging

from datetime import datetime

import pytest
import pandas as pd
import pyodbc

from mssql_dataframe.connect import connect
from mssql_dataframe.core import conversion, create
from mssql_dataframe.__equality__ import compare_dfs

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
    
    expected = pd.DataFrame.from_records([
        {
            "column_name": "_index", "sql_type": "tinyint", "is_nullable": False,
            "ss_is_identity": False, "pk_seq": 1, "pandas_type": "UInt8",
            "odbc_type": pyodbc.SQL_TINYINT, "odbc_size": 1, "odbc_precision": 0
        },
        {
            "column_name": "_pk", "sql_type": "int identity", "is_nullable": False,
            "ss_is_identity": True, "pk_seq": 1, "pandas_type": "Int32",
            "odbc_type": pyodbc.SQL_INTEGER, "odbc_size": 4, "odbc_precision": 0
        },
        {
            "column_name": "_char", "sql_type": "char", "is_nullable": True,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "string",
            "odbc_type": pyodbc.SQL_CHAR, "odbc_size": 0, "odbc_precision": 0
        },
        {
            "column_name": "_tinyint", "sql_type": "tinyint", "is_nullable": True,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "UInt8",
            "odbc_type": pyodbc.SQL_TINYINT, "odbc_size": 1, "odbc_precision": 0
        },
        {
            "column_name": "_smallint", "sql_type": "smallint", "is_nullable": False,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "Int16",
            "odbc_type": pyodbc.SQL_SMALLINT, "odbc_size": 2, "odbc_precision": 0
        },
        {
            "column_name": "_int", "sql_type": "int", "is_nullable": False,
            "ss_is_identity": False, "pk_seq":pd.NA, "pandas_type": "Int32",
            "odbc_type": pyodbc.SQL_INTEGER, "odbc_size": 4, "odbc_precision": 0
        },
        {
            "column_name": "_bigint", "sql_type": "bigint", "is_nullable": True,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "Int64",
            "odbc_type": pyodbc.SQL_BIGINT, "odbc_size": 8, "odbc_precision": 0
        },
        {
            "column_name": "_float", "sql_type": "float", "is_nullable": False,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "float64",
            "odbc_type": pyodbc.SQL_FLOAT, "odbc_size": 8, "odbc_precision": 53
        },
        {
            "column_name": "_time", "sql_type": "time", "is_nullable": False,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "timedelta64[ns]",
            "odbc_type": pyodbc.SQL_SS_TIME2, "odbc_size": 16, "odbc_precision": 7
        },
        {
            "column_name": "_datetime", "sql_type": "datetime2", "is_nullable": True,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "datetime64[ns]",
            "odbc_type": pyodbc.SQL_TYPE_TIMESTAMP, "odbc_size": 27, "odbc_precision": 7
        },
        {
            "column_name": "_empty", "sql_type": "nvarchar", "is_nullable": True,
            "ss_is_identity": False, "pk_seq": pd.NA, "pandas_type": "string",
            "odbc_type": pyodbc.SQL_WVARCHAR, "odbc_size": 0, "odbc_precision": 0
        },
    ])

    columns = ['column_name', 'sql_type', 'pandas_type']
    expected[columns] = expected[columns].astype('string')
    expected[['pk_seq']] = expected[['pk_seq']].astype('Int64')

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
        pd.Series(
            [1, pd.NA], index=pd.Series(["_pk", "A"], dtype="string"), dtype="Int64"
        )
    )
    assert all(schema["pk_name"].isna() == [False, True])
    assert all(schema["pandas_type"] == ["Int32", "string"])
    assert all(schema["odbc_type"] == [pyodbc.SQL_INTEGER, pyodbc.SQL_VARCHAR])
    assert all(schema["odbc_size"] == [4, 0])
    assert all(schema["odbc_precision"] == [0, 0])


def test_table_from_dataframe_simple(sql, caplog):

    table_name = "##test_table_from_dataframe_simple"
    dataframe = pd.DataFrame({"ColumnA": [1]})
    dataframe = sql.create.table_from_dataframe(table_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert len(schema) == 1
    assert all(schema.index == "ColumnA")
    assert all(schema["sql_type"] == "tinyint")
    assert all(~schema["is_nullable"])
    assert all(~schema["ss_is_identity"])
    assert all(schema["pk_seq"].isna())
    assert all(schema["pk_name"].isna())
    assert all(schema["pandas_type"] == "UInt8")
    assert all(schema["odbc_type"] == pyodbc.SQL_TINYINT)
    assert all(schema["odbc_size"] == 1)
    assert all(schema["odbc_precision"] == 0)

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result.equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_datestr(sql, caplog):
    table_name = "##test_table_from_dataframe_datestr"
    dataframe = pd.DataFrame({"ColumnA": ["06/22/2021"]})
    dataframe = sql.create_meta.table_from_dataframe(table_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, table_name)

    expected = pd.DataFrame(
        {
            "column_name": pd.Series(["ColumnA", "_time_insert"], dtype="string"),
            "sql_type": pd.Series(["date", "datetime2"], dtype="string"),
            "is_nullable": pd.Series([False, True]),
            "ss_is_identity": pd.Series([False, False]),
            "pk_seq": pd.Series([None, None], dtype="Int64"),
            "pk_name": pd.Series([None, None], dtype="string"),
            "pandas_type": pd.Series(
                ["datetime64[ns]", "datetime64[ns]"], dtype="string"
            ),
            "odbc_type": pd.Series(
                [pyodbc.SQL_TYPE_DATE, pyodbc.SQL_TYPE_TIMESTAMP], dtype="int64"
            ),
            "odbc_size": pd.Series([10, 27], dtype="int64"),
            "odbc_precision": pd.Series([0, 7], dtype="int64"),
        }
    ).set_index(keys="column_name")
    assert schema[expected.columns].equals(expected)

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_errorpk(sql, sample):

    with pytest.raises(ValueError):
        table_name = "##test_table_from_dataframe_nopk"
        sql.create.table_from_dataframe(table_name, sample, primary_key="ColumnName")


def test_table_from_dataframe_nopk(sql, sample, validation, caplog):

    table_name = "##test_table_from_dataframe_nopk"
    dataframe = sql.create.table_from_dataframe(
        table_name, sample.copy(), primary_key=None
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    validation = validation.drop(['_pk','_index'])
    compare_dfs(schema[validation.columns], validation.loc[schema.index])

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_sqlpk(sql, sample, validation, caplog):

    table_name = "##test_table_from_dataframe_sqlpk"
    dataframe = sql.create.table_from_dataframe(
        table_name, sample.copy(), primary_key="sql"
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    validation = validation.drop('_index')
    assert schema[validation.columns].equals(validation.loc[schema.index])
    assert pd.notna(schema.at["_pk", "pk_name"])
    assert schema.loc[schema.index != "_pk", "pk_name"].isna().all()

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    result = result.reset_index(drop=True)
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_indexpk_unnamed(sql, sample, validation, caplog):

    table_name = "##test_table_from_dataframe_indexpk_unnamed"
    dataframe = sql.create.table_from_dataframe(
        table_name, sample.copy(), primary_key="index"
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    validation = validation.drop('_pk')
    assert compare_dfs(schema[validation.columns], validation.loc[schema.index])
    assert pd.notna(schema.at["_index", "pk_name"])
    assert schema.loc[schema.index != "_index", "pk_name"].isna().all()

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_indexpk_named(sql, sample, validation, caplog):

    table_name = "##test_table_from_dataframe_indexpk_named"
    sample.index.name = "NamedIndex"
    dataframe = sql.create.table_from_dataframe(
        table_name, sample.copy(), primary_key="index"
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    validation = validation.drop('_pk')
    index = validation.index.values
    index[index=='_index'] = 'NamedIndex'
    validation.index = index
    assert compare_dfs(schema[validation.columns], validation.loc[schema.index])
    assert pd.notna(schema.at["NamedIndex", "pk_name"])
    assert schema.loc[schema.index != "NamedIndex", "pk_name"].isna().all()

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_inferpk_integer(sql, caplog):

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
    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="infer"
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert schema.at["_smallint", "pk_seq"] == 1
    assert all(schema.loc[schema.index != "_smallint", "pk_seq"].isna())

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe.sort_index())

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_inferpk_string(sql, caplog):

    table_name = "##test_table_from_dataframe_inferpk_string"
    dataframe = pd.DataFrame(
        {
            "_varchar1": ["a", "b", "c", "d", "e"],
            "_varchar2": ["aa", "b", "c", "d", "e"],
        }
    )
    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="infer"
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert schema.at["_varchar1", "pk_seq"] == 1
    assert all(schema.loc[schema.index != "_varchar1", "pk_seq"].isna())

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_inferpk_none(sql, caplog):

    table_name = "##test_table_from_dataframe_inferpk_none"
    dataframe = pd.DataFrame(
        {
            "_varchar1": [None, "b", "c", "d", "e"],
            "_varchar2": [None, "b", "c", "d", "e"],
        }
    )

    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="infer"
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert all(schema["pk_seq"].isna())

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]


def test_table_from_dataframe_composite_pk(sql, caplog):

    table_name = "##test_table_from_dataframe_composite_pk"
    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2], "ColumnB": ["a", "b"], "ColumnC": [3, 4]}
    )
    dataframe = dataframe.set_index(keys=["ColumnA", "ColumnB"])
    dataframe = sql.create.table_from_dataframe(
        table_name, dataframe, primary_key="index"
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)

    assert schema.at["ColumnA", "pk_seq"] == 1
    assert schema.at["ColumnB", "pk_seq"] == 2

    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)

    # assert warnings raised by logging after all other tasks
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples[0][0] == "mssql_dataframe.core.create"
    assert caplog.record_tuples[0][1] == logging.WARNING
    assert "Created table" in caplog.record_tuples[0][2]
