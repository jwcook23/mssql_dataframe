import env

import pandas as pd

import pytest

from mssql_dataframe.connect import connect
from mssql_dataframe.core import conversion

pd.options.mode.chained_assignment = "raise"


@pytest.fixture(scope="module")
def sql():
    # create database cursor
    db = connect(env.database, env.server, env.driver, env.username, env.password)

    # database cursor
    cursor = db.connection.cursor()

    # create table
    create = """
    CREATE TABLE ##test_conversion_error (
    id BIGINT PRIMARY KEY,
    _bit BIT
    )"""
    cursor.execute(create)

    yield db.connection
    db.connection.close()


def test_larger_sql_range():
    # error for a time value outside of allowed range
    with pytest.raises(ValueError):
        conversion.prepare_values(
            pd.DataFrame(["time"], columns=["sql_type"], index=["ColumnA"]),
            pd.DataFrame({"ColumnA": [pd.Timedelta(days=2)]}),
        )


def test_read_values_errors(sql):
    schema, _ = conversion.get_schema(
        connection=sql, table_name="##test_conversion_error"
    )
    # error for a column missing in schema definition
    with pytest.raises(AttributeError):
        conversion.read_values(
            statement="SELECT * FROM ##test_conversion_error",
            schema=schema[schema.index != "id"],
            connection=sql,
        )
    # error for primary key missing from query statement
    with pytest.raises(KeyError):
        conversion.read_values(
            statement="SELECT _bit FROM ##test_conversion_error",
            schema=schema,
            connection=sql,
        )
