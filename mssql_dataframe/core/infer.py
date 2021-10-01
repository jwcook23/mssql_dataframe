"""Functions for inferring best SQL and datarame data types based on dataframe contents that
may be in objects/strings. Also contains functions for determining other SQL properties.
"""

from mssql_dataframe.core import conversion_rules, errors

from datetime import time
from typing import Tuple

import pandas as pd

pd.options.mode.chained_assignment = "raise"


def sql(dataframe: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Index, str]:
    """Infer best fit data types using dataframe values. May be an object converted to a better type,
    or numeric values downcasted to a smallter data type.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : contains unconverted and non-downcasted columns

    Returns
    -------
    dataframe (pandas.DataFrame) : contains columns converted to best fit pandas data type
    schema (pandas.DataFrame) : derived SQL schema
    not_nullable (pandas.Index) : columns that should not be null
    pk (str) : name of column that best fits as the primary key
    """

    # numeric like: bit, tinyint, smallint, int, bigint, float
    dataframe = convert_numeric(dataframe)

    # datetime like: time, date, datetime2
    dataframe = convert_date(dataframe)

    # string like: varchar, nvarchar
    dataframe = convert_string(dataframe)

    # determine SQL properties
    schema = sql_schema(dataframe)
    not_nullable, pk = sql_unique(dataframe, schema)

    return dataframe, schema, not_nullable, pk


def convert_numeric(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert objects or numerics to downcasted nullable boolean, nullable integer, or nullable float data type if possible.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : contains unconverted and non-downcasted columns

    Returns
    -------
    dataframe (pandas.DataFrame) : contains possibly converted or downcasted columns
    """

    # attempt conversion of strings/numeric to a downcasted numeric
    columns = dataframe.select_dtypes(
        include=["object", "number"], exclude="timedelta"
    ).columns
    dataframe[columns] = dataframe[columns].replace({"True": "1", "False": "0"})
    for col in columns:
        # skip missing since pd.to_numeric doesn't work with nullable integer types
        notna = ~dataframe[col].isna()
        if (~notna).all():
            continue
        try:
            converted = pd.to_numeric(dataframe.loc[notna, col], downcast="integer")
            dataframe.loc[notna, col] = converted
            name = converted.dtype.name
            # convert to nullable integer type
            if converted.dtype.name.startswith("int"):
                name = name.capitalize()
            dataframe[col] = dataframe[col].astype(name)
        except Exception:
            continue

    # convert Int8 to nullable boolean if multiple values of only 0,1, or NA
    columns = [k for k, v in dataframe.dtypes.items() if v.name == "Int8"]
    # insure conversion doesn't change values outside of range to limit of 0 or 1
    converted = dataframe[columns].astype("boolean")
    skip = (~(dataframe[columns] == converted)).any()
    # insure there are multiple instances of 0 or 1
    multiple = dataframe[columns].isin([0, 1]).sum() > 2
    # convert if rules upheld
    columns = [
        x
        for x in columns
        if x not in skip[skip].index and x in multiple[multiple].index
    ]
    dataframe[columns] = dataframe[columns].astype("boolean")

    # # convert Int8/Int16 to UInt8 (0-255 to bring inline with SQL TINYINT)
    columns = [
        k for k, v in dataframe.dtypes.items() if v.name == "Int8" or v.name == "Int16"
    ]
    # insure conversion doesn't change values outside of range to limit of 0 or 255
    converted = dataframe[columns].astype("UInt8")
    skip = (~(dataframe[columns] == converted)).any()
    # convert if rules are upheld
    columns = [x for x in columns if x not in skip[skip].index]
    dataframe[columns] = dataframe[columns].astype("UInt8")

    return dataframe


def convert_date(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert objects to nullable time delta or nullable datetime data type if possible.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : contains unconverted columns

    Returns
    -------
    dataframe (pandas.DataFrame) : contains possibly converted columns
    """

    # attempt conversion of object columns to timedelta
    columns = dataframe.columns[dataframe.dtypes == "object"]
    for col in columns:
        if dataframe[col].isna().all():
            continue
        dataframe[col] = pd.to_timedelta(dataframe[col], errors="ignore")
    # attempt conversion of object columns to datetime
    columns = dataframe.columns[dataframe.dtypes == "object"]
    for col in columns:
        if dataframe[col].isna().all():
            continue
        dataframe[col] = pd.to_datetime(dataframe[col], errors="ignore")

    return dataframe


def convert_string(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert objects to nullable string data type.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : contains unconverted columns

    Returns
    -------
    dataframe (pandas.DataFrame) : contains columns possibly converted to nullable string
    """

    columns = dataframe.columns[dataframe.dtypes == "object"]
    dataframe[columns] = dataframe[columns].astype("string")

    return dataframe


def sql_unique(dataframe: pd.DataFrame, schema: pd.DataFrame) -> Tuple[pd.Index, str]:
    """Determine if columns should be nullable in SQL and determine best fitting primary key column.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : columns to check
    schema (pandas.DataFrame) : column definitions for SQL schema

    Returns
    -------
    not_nullable (pandas.Index) : columns that should not be null
    pk (str) : name of column that best fits as the primary key

    """

    # determine columns not nullable
    not_nullable = dataframe.notna().all()
    not_nullable = not_nullable[not_nullable].index

    # primary key can't be null
    dataframe = dataframe[not_nullable]

    # primary key must be all unique values
    unique = dataframe.nunique() == len(dataframe)
    dataframe = dataframe[unique[unique].index]

    # primary key
    schema = schema.loc[dataframe.columns]
    schema.index.name = "column_name"
    schema = schema.reset_index()
    # attempt to use smallest sized numeric value
    check = pd.Series(["tinyint", "smallint", "int", "bigint"], name="sql_type")
    pk = pd.DataFrame(check).merge(schema, left_on="sql_type", right_on="sql_type")
    if len(pk) > 0:
        pk = dataframe[pk["column_name"]].max().idxmin()
    else:
        pk = None
    # attempt to use smallest size string value
    if pk is None:
        check = pd.Series(["varchar", "nvarchar"], name="sql_type")
        pk = pd.DataFrame(check).merge(schema, left_on="sql_type", right_on="sql_type")
        if len(pk) > 0:
            pk = (
                dataframe[pk["column_name"]].apply(lambda x: x.str.len().max()).idxmin()
            )
        else:
            pk = None

    return not_nullable, pk


def sql_schema(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Determine SQL data type based on pandas data type.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : data to determine SQL type for

    Returns
    -------
    schema (pandas.DataFrame) : derived SQL schema

    """

    # determine data type based on conversion rules
    schema = pd.DataFrame(dataframe.dtypes.copy(), columns=["pandas_type"])
    schema.index.name = "column_name"
    schema = schema.reset_index()
    schema["pandas_type"] = schema["pandas_type"].apply(lambda x: x.name)
    schema = schema.merge(
        conversion_rules.rules,
        left_on="pandas_type",
        right_on="pandas_type",
        how="left",
    )
    missing = schema.isna().any(axis="columns")
    if any(missing):
        missing = schema.loc[missing, "column_name"].to_list()
        raise errors.UndefinedConversionRule(f"columns: {missing}")
    schema = schema.set_index(keys="column_name")

    # determine SQL type for pandas string
    schema = _deduplicate_string(dataframe, schema)

    # determine SQL type for pandas datetime64[ns]
    schema = _deduplicate_datetime(dataframe, schema)

    # insure schema is in same order ad dataframe columns
    schema = schema.loc[dataframe.columns]
    schema.index.name = "column_name"

    return schema


def _deduplicate_string(dataframe: pd.DataFrame, schema: pd.DataFrame) -> pd.DataFrame:
    """Determine if pandas string should be SQL varchar or nvarchar.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : data to resolve
    schema (pandas.DataFrame) : conversion information for each column

    Return
    ------
    schema (pandas.DataFrame) : resolved data types
    """

    deduplicate = schema[schema["pandas_type"] == "string"]
    columns = deduplicate.index.unique()
    for col in columns:
        # if encoding removes characters or all are None then assume nvarchar
        pre = dataframe[col].str.len()
        post = (
            dataframe[col]
            .str.encode("ascii", errors="ignore")
            .str.len()
            .astype("Int64")
        )
        if pre.ne(post).any() or dataframe[col].isna().all():
            resolved = deduplicate[deduplicate["sql_type"] == "nvarchar"].loc[col]
        else:
            resolved = deduplicate[deduplicate["sql_type"] == "varchar"].loc[col]
        # add resolution into schema
        schema = schema[schema.index != col]
        schema = schema.append(resolved)

    return schema


def _deduplicate_datetime(
    dataframe: pd.DataFrame, schema: pd.DataFrame
) -> pd.DataFrame:
    """Determine if pandas datetime should be SQL date or datetime2.

    Parameters
    ----------
    dataframe (pandas.DataFrame) : data to resolve
    schema (pandas.DataFrame) : conversion information for each column

    Return
    ------
    schema (pandas.DataFrame) : resolved data types
    """

    deduplicate = schema[schema["pandas_type"] == "datetime64[ns]"]
    columns = deduplicate.index.unique()
    for col in columns:
        # if all time components are zero then assume date
        if (dataframe[col].dt.time.fillna(time(0, 0)) == time(0, 0)).all():
            resolved = deduplicate[deduplicate["sql_type"] == "date"].loc[col]
        else:
            resolved = deduplicate[deduplicate["sql_type"] == "datetime2"].loc[col]
        # add resolution into schema
        schema = schema[schema.index != col]
        schema = schema.append(resolved)

    return schema
