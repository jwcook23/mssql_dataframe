import warnings

import pandas as pd

import pytest
import pyodbc

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_warnings, conversion, conversion_rules, dynamic
from . import sample

pd.options.mode.chained_assignment = "raise"

@pytest.fixture(scope="module")
def data():
    """Sample data for supported data types.

    See Also: sample.py
    """

    df = sample.dataframe

    # add id column to guarantee SQL read return order
    df.index.name = "id"
    df = df.reset_index()
    df["id"] = df["id"].astype("Int64")

    return df


@pytest.fixture(scope="module")
def sql(data):
    # create database cursor
    db = connect(database="tempdb", server="localhost")

    # database cursor
    cursor = db.connection.cursor()

    # column name = dataframe column name, data type = dataframe column name without prefixed underscore
    create = {x: x[1::].upper() for x in data.columns if x != "id"}
    # use string lengths to determine string column size
    create = {
        k: (v + "(" + str(data[k].str.len().max()) + ")" if v == "VARCHAR" else v)
        for k, v in create.items()
    }
    create = {
        k: (v + "(" + str(data[k].str.len().max()) + ")" if v == "NVARCHAR" else v)
        for k, v in create.items()
    }
    # create SQL table, including a primary key for SQL read sorting
    create = ",\n".join([k + " " + v for k, v in create.items()])
    create = f"""
    CREATE TABLE ##test_conversion (
    id BIGINT PRIMARY KEY,
    {create}
    )"""
    cursor.execute(create)

    yield db.connection
    db.connection.close()


def test_rules(data):

    # insure conversion rules are fully defined
    defined = conversion_rules.rules.notna().all()
    try:
        assert all(defined)
    except AssertionError as error:
        missing = defined[~defined].index.tolist()
        missing = f"conversion not fully defined for: {missing}"
        error.args += missing

    # insure sample contains all conversion rules
    defined = conversion_rules.rules["sql_type"].isin([x[1::] for x in data.columns])
    try:
        assert all(defined)
    except AssertionError as error:
        missing = conversion_rules.rules.loc[missing, "sql_type"].tolist()
        missing = f"columns missing from sample dataframe, SQL column names: {missing}"
        error.args += missing

    # insure sample contains correct pandas types
    check = pd.Series(data.dtypes, name="sample")
    check = check.apply(lambda x: x.name)
    check.index = [x[1::] for x in check.index]
    check = conversion_rules.rules.merge(check, left_on="sql_type", right_index=True)
    defined = check["pandas_type"] == check["sample"]
    try:
        assert all(defined)
    except AssertionError as error:
        missing = check.loc[~defined, "sql_type"].tolist()
        missing = f"columns with wrong data type in sample dataframe, SQL column names: {missing}"
        error.args += missing


def test_sample(sql, data):

    # create cursor to perform operations
    cursor = sql.cursor()
    cursor.fast_executemany = True

    # get table schema for setting input data types and sizes
    schema, dataframe = conversion.get_schema(
        connection=sql, table_name="##test_conversion"
    )

    # dynamic SQL object names
    table = dynamic.escape(cursor, "##test_conversion")
    columns = dynamic.escape(cursor, data.columns)

    # prepare values of dataframe for insert
    with warnings.catch_warnings(record=True) as warn:
        dataframe, values = conversion.prepare_values(schema, data)
        assert len(warn) == 2
        assert isinstance(warn[0].message, custom_warnings.SQLDataTypeTIMETruncation)
        assert (
            str(warn[0].message)
            == "Nanosecond precision for dataframe columns ['_time'] will be truncated as SQL data type TIME allows 7 max decimal places."
        )
        assert isinstance(warn[1].message, custom_warnings.SQLDataTypeDATETIME2Truncation)
        assert (
            str(warn[1].message)
            == "Nanosecond precision for dataframe columns ['_datetime2'] will be truncated as SQL data type DATETIME2 allows 7 max decimal places."
        )

    # prepare cursor for input data types and sizes
    cursor = conversion.prepare_cursor(schema, dataframe, cursor)

    # issue insert statement
    insert = ", ".join(columns)
    params = ", ".join(["?"] * len(columns))
    statement = f"""
    INSERT INTO
    {table} (
        {insert}
    ) VALUES (
        {params}
    )
    """
    cursor.executemany(statement, values)

    # read data, excluding ID columns that is only to insure sorting
    columns = ", ".join([x for x in data.columns])
    statement = f"SELECT {columns} FROM {table} ORDER BY id ASC"
    result = conversion.read_values(statement, schema, connection=sql)

    # compare result to insert
    ## note comparing to dataframe as values may have changed during insert preperation
    assert result.equals(dataframe.set_index(keys="id"))


def test_prepare_values_errors():

    schema = pd.DataFrame({"odbc_type": pd.Series([pyodbc.SQL_SS_TIME2])})
    schema.index = ["ColumnA"]
    dataframe = pd.DataFrame({"ColumnA": [pd.Timedelta(days=2)]})
    # error for a time value outside of allowed range
    with pytest.raises(ValueError):
        conversion.prepare_values(schema, dataframe)


def test_read_values_errors(sql):

    schema, _ = conversion.get_schema(connection=sql, table_name="##test_conversion")
    # error for a column missingin schema definition
    with pytest.raises(AttributeError):
        conversion.read_values(
            statement="SELECT * FROM ##test_conversion",
            schema=schema[schema.index != "id"],
            connection=sql,
        )
    # error for primary key missing from query statement
    with pytest.raises(KeyError):
        conversion.read_values(
            statement="SELECT _bit FROM ##test_conversion",
            schema=schema,
            connection=sql,
        )
