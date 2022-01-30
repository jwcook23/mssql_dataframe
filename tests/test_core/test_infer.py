import pandas as pd
import numpy as np
import pytest

from mssql_dataframe.core import custom_errors, infer
from mssql_dataframe import __sample__

pd.options.mode.chained_assignment = "raise"


@pytest.fixture(scope="module")
def data():
    """Sample data for supported data types.

    See Also: mssql_dataframe/__sample__.py
    """

    df = __sample__.dataframe

    return df


def _check_schema(dtypes):
    """Assert expected dtypes are inferred.

    Dataframe column names should be in the form _SQLDataType or _SQLDataType_SomeOtherText.
    """
    expected = dtypes["sql_type"].reset_index()
    expected["actual"] = expected["column_name"].str.split("_")
    expected["actual"] = expected["actual"].apply(lambda x: x[1])
    assert (expected["sql_type"] == expected["actual"]).all()


def _check_dataframe(dataframe, dtypes):
    """Assert dataframe columns are of the correct type."""
    expected = dataframe.dtypes.apply(lambda x: x.name)
    expected.name = "pandas_type"
    expected.index.name = "column_name"
    expected = expected.reset_index()
    expected["column_name"] = expected["column_name"].astype("string")
    expected = expected.sort_values(by="column_name", ignore_index=True)
    actual = dtypes["pandas_type"].reset_index()
    actual = actual.sort_values(by="column_name", ignore_index=True)
    assert actual.equals(expected)


def test_dtypes(data):

    # setup test data
    dataframe = data.copy()
    na = dataframe.isna()
    dataframe = dataframe.astype("str")
    dataframe[na] = None
    dataframe["_time"] = dataframe["_time"].str.replace("0 days ", "")

    # infer SQL properties
    dataframe, schema, not_nullable, pk = infer.sql(dataframe)

    # assert inferred results
    _check_schema(schema)
    _check_dataframe(dataframe, schema)
    assert len(not_nullable) == 0
    assert pk is None


def test_pk(data):

    # setup test data
    dataframe = data[data.notna().all(axis="columns")].copy()
    dataframe["_tinyint_smaller"] = pd.Series(range(0, len(dataframe)), dtype="UInt8")
    dataframe["_varchar_smaller"] = dataframe["_varchar"].str.slice(0, 1)

    # infer SQL properties
    df = dataframe
    df, schema, not_nullable, pk = infer.sql(df)
    _check_schema(schema)
    _check_dataframe(df, schema)
    assert df.columns.isin(not_nullable).all()
    assert pk == "_tinyint_smaller"

    # infer SQL properties without numeric
    df = dataframe.select_dtypes(["datetime", "string"])
    df, schema, not_nullable, pk = infer.sql(df)
    _check_schema(schema)
    _check_dataframe(df, schema)
    assert df.columns.isin(not_nullable).all()
    assert pk == "_varchar_smaller"


def test_default(data):

    # setup test data
    dataframe = data.copy()
    dataframe["_nvarchar_default1"] = None
    dataframe["_nvarchar_default2"] = np.nan

    # infer SQL properties
    dataframe, schema, not_nullable, pk = infer.sql(dataframe)
    _check_schema(schema)
    _check_dataframe(dataframe, schema)
    assert len(not_nullable) == 0
    assert pk is None


def test_sql_schema_errors():

    dataframe = pd.DataFrame({"ColumnA": pd.Series([1, 2, 3], dtype="category")})
    with pytest.raises(custom_errors.UndefinedConversionRule):
        infer.sql_schema(dataframe)
