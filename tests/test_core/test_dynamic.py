import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_errors, dynamic

pd.options.mode.chained_assignment = "raise"


@pytest.fixture(scope="module")
def cursor():
    # create database cursor
    db = connect(database="tempdb", server="localhost")

    # database cursor
    cursor = db.connection.cursor()
    yield cursor


def test_escape(cursor):
    # list of values
    inputs = [
        "TableName",
        "##TableName",
        "ColumnName",
        "'; select true; --",
        "abc[]def",
        "user's custom name",
    ]
    clean = dynamic.escape(cursor, inputs)
    assert isinstance(inputs, list)
    assert len(clean) == len(inputs)

    # single string
    inputs = "SingleString"
    clean = dynamic.escape(cursor, inputs)
    assert isinstance(inputs, str)

    # dataframe columns (pandas index)
    dataframe = pd.DataFrame(columns=["A", "B"])
    clean = dynamic.escape(cursor, dataframe.columns)
    assert len(clean) == dataframe.shape[1]

    # schema specification list
    inputs = ["test.dbo.table", "tempdb..##table"]
    clean = dynamic.escape(cursor, inputs)
    assert len(clean) == len(inputs)

    # schema specification single string
    inputs = "test.dbo.table"
    clean = dynamic.escape(cursor, inputs)
    assert isinstance(clean, str)

    # value that is too long
    with pytest.raises(custom_errors.SQLInvalidLengthObjectName):
        dynamic.escape(cursor, inputs="a" * 1000)


def test_where(cursor):
    where = "ColumnA >5 AND ColumnB=2 and ColumnANDC IS NOT NULL"
    where_statement, where_args = dynamic.where(cursor, where)
    assert (
        where_statement
        == "WHERE [ColumnA] > ? AND [ColumnB] = ? and [ColumnANDC] IS NOT NULL"
    )
    assert where_args == ["5", "2"]

    where = "ColumnA <>5 AND ColumnB!=2 and ColumnANDC IS NOT NULL"
    where_statement, where_args = dynamic.where(cursor, where)
    assert (
        where_statement
        == "WHERE [ColumnA] <> ? AND [ColumnB] != ? and [ColumnANDC] IS NOT NULL"
    )
    assert where_args == ["5", "2"]

    where = "ColumnB>4 AND ColumnC IS NOT NULL OR ColumnD IS NULL"
    where_statement, where_args = dynamic.where(cursor, where)
    assert (
        where_statement
        == "WHERE [ColumnB] > ? AND [ColumnC] IS NOT NULL OR [ColumnD] IS NULL"
    )
    assert where_args == ["4"]

    where = "ColumnA IS NULL OR ColumnA != 'CLOSED'"
    where_statement, where_args = dynamic.where(cursor, where)
    assert where_statement == "WHERE [ColumnA] IS NULL OR [ColumnA] != ?"
    assert where_args == ["CLOSED"]

    conditions = "no operator present"
    with pytest.raises(custom_errors.SQLInvalidSyntax):
        dynamic.where(cursor, conditions)
