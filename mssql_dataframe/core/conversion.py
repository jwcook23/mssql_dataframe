""" Functions for data movement between Python pandas dataframes and SQL."""
from mssql_dataframe.core import errors, conversion_rules

import warnings
import struct
from typing import Tuple, List

import pyodbc
import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = "raise"


def get_schema(
    connection: pyodbc.connect,
    table_name: str,
    dataframe: pd.DataFrame = None,
    additional_columns: List[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get schema of an SQL table and the defined conversion rules between data types.

    If a dataframe is provided, also checks the contents of the dataframe for the ability
    to write to the SQL table and raises approriate exceptions if needed. Additionally
    converts the data types of the dataframe according to the conversion rules.

    Parameters
    ----------
    connection (pyodbc.connect) : connection to database
    table_name (str) : table name containing columns
    dataframe (pandas.DataFrame, default=None) : check contents against schema and convert using rules
    additional_columns (list, default=None) : columns that will be generated by an SQL statement but not in the dataframe, such as metadata columns

    Returns
    -------
    schema (pandas.DataFrame) : table column specifications and conversion rules
    dataframe (pandas.DataFrame) : dataframe with contents converted to conform to SQL data type

    """

    cursor = connection.cursor()

    # add cataglog for temporary tables
    if table_name.startswith("#"):
        catalog = "tempdb"
    else:
        catalog = None

    # get schema
    schema = []
    cursor = cursor.columns(table=table_name, catalog=catalog)
    for col in cursor:
        schema.append(list(col))
    schema = pd.DataFrame(schema, columns=[x[0] for x in cursor.description])
    # check for no SQL table
    if len(schema) == 0:
        raise errors.SQLTableDoesNotExist(
            f"catalog = {catalog}, table_name = {table_name}"
        )
    # check for missing columns not expected to be in dataframe
    # such as include_metadata_timestamps columns like _time_insert or _time_update
    # perform check seperately to insure this is raised without other dataframe columns
    if additional_columns is not None:
        columns = pd.Series(additional_columns, dtype="string")
        missing = columns[~columns.isin(schema["column_name"])]
        if len(missing) > 0:
            missing = list(missing)
            raise errors.SQLColumnDoesNotExist(
                f"catalog = {catalog}, table_name = {table_name}, columns={missing}",
                missing,
            )
    # check for other missing columns
    if dataframe is not None:
        columns = dataframe.columns
        missing = columns[~columns.isin(schema["column_name"])]
        if len(missing) > 0:
            missing = list(missing)
            raise errors.SQLColumnDoesNotExist(
                f"catalog = {catalog}, table_name = {table_name}, columns={missing}",
                missing,
            )
    # format schema
    schema = schema.rename(columns={"type_name": "sql_type"})
    schema = schema[
        [
            "column_name",
            "data_type",
            "column_size",
            "sql_type",
            "is_nullable",
            "ss_is_identity",
        ]
    ]
    schema[["column_name", "sql_type"]] = schema[["column_name", "sql_type"]].astype(
        "string"
    )
    schema["is_nullable"] = schema["is_nullable"] == "YES"
    schema["ss_is_identity"] = schema["ss_is_identity"] == 1

    # add primary key info
    pk = cursor.primaryKeys(table=table_name, catalog=catalog).fetchall()
    pk = pd.DataFrame([list(x) for x in pk], columns=[x[0] for x in cursor.description])
    pk = pk.rename(columns={"key_seq": "pk_seq"})
    schema = schema.merge(
        pk[["column_name", "pk_seq", "pk_name"]],
        left_on="column_name",
        right_on="column_name",
        how="left",
    )
    schema["pk_seq"] = schema["pk_seq"].astype("Int64")
    schema["pk_name"] = schema["pk_name"].astype("string")

    # add conversion rules
    identity = schema["sql_type"] == "int identity"
    schema.loc[identity, "sql_type"] = "int"
    schema = schema.merge(
        conversion_rules.rules, left_on="sql_type", right_on="sql_type", how="left"
    )
    schema.loc[identity, "sql_type"] = "int identity"

    # key column_name as index, check for undefined conversion rule
    schema = schema.set_index(keys="column_name")
    missing = schema[conversion_rules.rules.columns].isna().any(axis="columns")
    if any(missing):
        missing = missing[missing].index.tolist()
        raise errors.UndefinedConversionRule(
            "SQL data type conversion to pandas is not defined for columns:", missing
        )

    # check contents of dataframe against SQL schema & convert
    if dataframe is not None:
        dataframe = _precheck_dataframe(schema, dataframe)

    return schema, dataframe


def _precheck_dataframe(schema: pd.DataFrame, dataframe: pd.DataFrame) -> pd.DataFrame:
    """Checks the contents of the dataframe for the ability to write to the SQL table
    and raises approriate exceptions if needed. Additionally converts the data types of the
    dataframe according to the conversion rules.

    Parameters
    ----------
    schema (pandas.DataFrame) : contains definitions for data schema
    dataframe (pandas.DataFrame) : values to be written to SQL

    Returns
    -------
    dataframe (pandas.DataFrame) : converted according to SQL data type

    """

    # temporarily set dataframe index (primary key) as a column
    index = dataframe.index.names
    if any(index):
        dataframe = dataframe.reset_index()

    # only apply to columns in dataframe
    schema = schema[schema.index.isin(dataframe.columns)].copy()

    # convert objects to the largest sql_category type to allow for size check (avoids downcast such as UInt8 value of 10000 to 16)
    convert = dataframe.columns[dataframe.dtypes == "object"]
    try:
        # exact numeric
        columns = convert[schema.loc[convert, "sql_category"] == "exact numeric"]
        dataframe[columns] = dataframe[columns].astype("Int64")
        # approximate numeric
        columns = convert[schema.loc[convert, "sql_category"] == "approximate numeric"]
        dataframe[columns] = dataframe[columns].astype("float64")
        # date time
        columns = convert[schema.loc[convert, "sql_category"] == "date time"]
        dataframe[columns] = dataframe[columns].astype("string")
        # character string
        columns = convert[schema.loc[convert, "sql_category"] == "character string"]
        dataframe[columns] = dataframe[columns].astype("string")
    except TypeError:
        raise errors.DataframeInvalidDataType(
            "Dataframe columns cannot be converted based on their SQL data type",
            list(columns),
        )

    # check for insufficient column size, using min and max of dataframe contents
    check = dataframe.copy()
    strings = check.columns[check.dtypes == "string"]
    schema.loc[strings, "max_value"] = schema.loc[strings, "column_size"]
    check[strings] = check[strings].apply(lambda x: x.str.len())
    check = check.agg([min, max]).transpose()
    check = check.merge(
        schema[["min_value", "max_value"]], left_index=True, right_index=True
    )
    invalid = check[
        (check["min"] < check["min_value"]) | (check["max"] > check["max_value"])
    ]
    if len(invalid) > 0:
        invalid = invalid.astype("string")
        invalid["allowed"] = invalid["min_value"] + " to " + invalid["max_value"]
        invalid["actual"] = invalid["min"] + " to " + invalid["max"]
        columns = list(invalid.index)
        raise errors.SQLInsufficientColumnSize(
            f"columns: {columns}, allowed range: {list(invalid.allowed)}, actual range: {list(invalid.actual)}",
            columns,
        )

    # convert dataframe based on SQL type
    dataframe = dataframe.astype(schema["pandas_type"].to_dict())

    # set primary key column as dataframe's index
    if any(schema["pk_seq"].notna()):
        pk = schema["pk_seq"].sort_values()
        pk = list(pk[pk.notna()].index)
        dataframe = dataframe.set_index(keys=pk)

    return dataframe


def prepare_cursor(
    schema: pd.DataFrame, dataframe: pd.DataFrame, cursor: pyodbc.connect
) -> pyodbc.connect:
    """
    Prepare cursor data types and size for writting values to SQL.

    Parameters
    ----------
    schema (pandas.DataFrame) : output from get_schema function
    dataframe (pandas.DataFrame) : values to be written to SQL, used to determine size of string columns
    cursor (pyodbc.connect.cursor) : cursor to be used to write values

    Returns
    -------
    cursor (pyodbc.connect.cursor) : cursor with SQL data type and size parameters set
    """

    schema = schema[
        [
            "column_size",
            "min_value",
            "max_value",
            "sql_type",
            "odbc_type",
            "odbc_size",
            "odbc_precision",
        ]
    ]

    # insure columns are sorted correctly
    columns = list(dataframe.columns)
    index = dataframe.index.names
    if any(index):
        columns = list(index) + columns
    schema = schema.loc[columns]

    # use dataframe contents to determine size for strings
    schema, _ = sql_spec(schema, dataframe)

    # set SQL data type and size for cursor
    schema = schema[["odbc_type", "odbc_size", "odbc_precision"]].to_numpy().tolist()
    schema = [tuple(x) for x in schema]
    cursor.setinputsizes(schema)

    return cursor


def sql_spec(
    schema: pd.DataFrame, dataframe: pd.DataFrame
) -> Tuple[pd.DataFrame, dict]:
    """Provides dictionary mapping of column name to SQL data type. Data type includes the size of
    VARCHAR and NVARCHAR columns using dataframe contents.

    Parameters
    ----------
    schema (pandas.DataFrame) : contains the column size to update and the column sql to identify string columns
    dataframe (pandas.DataFrame) : dataframe contents

    Returns
    -------
    schema (pandas.DataFrame) : column 'odbc_size' set according to size of contents for string columns
    dtypes (dict) : dictionary mapping of each column SQL data type
    """

    if any(dataframe.index.names):
        dataframe = dataframe.reset_index()

    strings = schema[schema["sql_type"].isin(["varchar", "nvarchar"])].index

    # update odbc_size in schema
    infer = dataframe[strings].apply(lambda x: x.str.len()).max()
    infer = pd.DataFrame(infer, columns=["odbc_size"])
    infer["odbc_size"] = infer["odbc_size"].fillna(1)
    schema.update(infer)
    schema["odbc_size"] = schema["odbc_size"].astype("int64")

    # develop dictionary mapping of data types
    dtypes = schema.loc[:, ["sql_type", "odbc_size"]]
    dtypes["odbc_size"] = dtypes["odbc_size"].astype("string")
    dtypes.loc[strings, "sql_type"] = (
        dtypes.loc[strings, "sql_type"] + "(" + dtypes.loc[strings, "odbc_size"] + ")"
    )
    dtypes = dtypes["sql_type"].to_dict()

    return schema, dtypes


def prepare_values(
    schema: pd.DataFrame, dataframe: pd.DataFrame
) -> Tuple[pd.DataFrame, list]:
    """Prepare dataframe contents for writting values to SQL.

    Parameters
    ----------
    schema (pandas.DataFrame) : data schema definition
    dataframe (pandas.DataFrame) : dataframe that will be written to SQL

    Returns
    -------
    dataframe (pandas.DataFrame) : values that may be altered to conform to SQL precision limitations
    values (list) : values to pass to pyodbc.connect.cursor.executemany

    """

    # create a copy to preserve values in return
    prepped = dataframe.copy()

    # include index as column as it is the primary key
    if any(prepped.index.names):
        prepped = prepped.reset_index()

    # only prepare values currently in dataframe
    schema = schema[schema.index.isin(prepped.columns)]

    # SQL data type TIME as string since python datetime.time allows 6 decimal places but SQL allows 7
    dtype = schema[schema["odbc_type"] == pyodbc.SQL_SS_TIME2].index
    truncation = prepped[dtype].apply(lambda x: any(x.dt.nanoseconds % 100 > 0))
    if any(truncation):
        truncation = list(truncation[truncation].index)
        warnings.warn(
            f"Nanosecond precision for dataframe columns {truncation} will be truncated as SQL data type TIME allows 7 max decimal places."
        )
        nanosecond = dataframe[dtype].apply(
            lambda x: pd.to_timedelta((x.dt.nanoseconds // 100) * 100)
        )
        dataframe[dtype] = dataframe[dtype].apply(lambda x: x.dt.floor(freq="us"))
        dataframe[dtype] = dataframe[dtype] + nanosecond
    invalid = (
        (prepped[dtype] >= pd.Timedelta(days=1))
        | (prepped[dtype] < pd.Timedelta(days=0))
    ).any()
    if any(invalid):
        invalid = list(invalid[invalid].index)
        raise ValueError(
            f"columns {invalid} are out of range for SQL TIME data type. Allowable range is 00:00:00.0000000-23:59:59.9999999"
        )
    prepped[dtype] = prepped[dtype].astype("str")
    prepped[dtype] = prepped[dtype].replace({"NaT": None})
    prepped[dtype] = prepped[dtype].apply(lambda x: x.str[7:23])

    # SQL data type DATETIME2 as string since python datetime.datetime allows 6 decimals but SQL allows 7
    dtype = schema[schema["odbc_type"] == pyodbc.SQL_TYPE_TIMESTAMP].index
    truncation = prepped[dtype].apply(lambda x: any(x.dt.nanosecond % 100 > 0))
    if any(truncation):
        truncation = list(truncation[truncation].index)
        warnings.warn(
            f"Nanosecond precision for dataframe columns {truncation} will be truncated as SQL data type DATETIME2 allows 7 max decimal places."
        )
        nanosecond = dataframe[dtype].apply(
            lambda x: pd.to_timedelta((x.dt.nanosecond // 100) * 100)
        )
        dataframe[dtype] = dataframe[dtype].apply(lambda x: x.dt.floor(freq="us"))
        dataframe[dtype] = dataframe[dtype] + nanosecond

    prepped[dtype] = prepped[dtype].astype("str")
    prepped[dtype] = prepped[dtype].replace({"NaT": None})
    prepped[dtype] = prepped[dtype].apply(lambda x: x.str[0:27])

    # treat pandas NA,NaT,etc as NULL in SQL
    prepped = prepped.fillna(np.nan).replace([np.nan], [None])

    # convert single column of datetime to objects
    # otherwise tolist() will produce ints instead of datetimes
    if prepped.shape[1] == 1 and prepped.select_dtypes("datetime").shape[1] == 1:
        prepped = prepped.astype(object)

    # values for pyodbc cursor executemany
    values = prepped.values.tolist()

    return dataframe, values


def prepare_connection(connection: pyodbc.connect) -> pyodbc.connect:
    """Prepare connection by adding output converters for data types directly to a pandas data type.

    1. Avoids errors such as pyodbc.ProgrammingError where the ODBC library doesn't already have a conversion defined.
    pyodbc.ProgrammingError: ('ODBC SQL type -155 is not yet supported. column-index=0 type=-155', 'HY106')

    2. Conversion directly to a pandas types allows greater precision. Python datetime.datetime allows 6
    decimal places of precision while pandas Timestamps allows 9.

    Note that adding converters for nullable pandas integer types isn't possible, since those are implemented at the
    array level. Pandas also doesn't support an exact precision decimal data type.

    Parameters
    ----------
    connection (pyodbc.connect) : connection without default output converters

    Returns
    -------
    connection (pyodbc.connect) : connection with added output converters
    """

    # TIME (pyodbc.SQL_SS_TIME2, SQL TIME)
    # python datetime.time has 6 decimal places of precision and isn't nullable
    # pandas Timedelta supports 9 decimal places and is nullable
    # SQL TIME only supports 7 decimal places for precision
    # SQL TIME range is '00:00:00.0000000' to '23:59:59.9999999' while pandas allows multiple days and negatives
    def SQL_SS_TIME2(raw_bytes, pattern=struct.Struct("<4hI")):
        hour, minute, second, _, fraction = pattern.unpack(raw_bytes)
        return pd.Timedelta(
            hours=hour,
            minutes=minute,
            seconds=second,
            microseconds=fraction // 1000,
            nanoseconds=fraction % 1000,
        )

    connection.add_output_converter(pyodbc.SQL_SS_TIME2, SQL_SS_TIME2)

    # TIMESTAMP (pyodbc.SQL_TYPE_TIMESTMAP, SQL DATETIME2)
    # python datetime.datetime has 6 decimal places of precision and isn't nullable
    # pandas Timestamp supports 9 decimal places and is nullable
    # SQL DATETIME2 only supports 7 decimal places for precision
    # pandas Timestamp range range is '1677-09-21 00:12:43.145225' to '2262-04-11 23:47:16.854775807' while SQL allows '0001-01-01' through '9999-12-31'
    def SQL_TYPE_TIMESTAMP(raw_bytes, pattern=struct.Struct("hHHHHHI")):
        year, month, day, hour, minute, second, fraction = pattern.unpack(raw_bytes)
        return pd.Timestamp(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=second,
            microsecond=fraction // 1000,
            nanosecond=fraction % 1000,
        )

    connection.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP)

    return connection


def read_values(
    statement: str, schema: pd.DataFrame, connection: pyodbc.connect, args: list = None
) -> pd.DataFrame:
    """Read data from SQL into a pandas dataframe.

    Parameters
    ----------
    statement (str) : statement to execute to get data
    schema (pandas.DataFrame) : output from get_schema function for setting dataframe data types
    connection (pyodbc.connect) : connection to database
    args (list, default=None) : arguments to pass for parameter placeholders

    Returns
    -------
    result (pandas.DataFrame) : resulting data from performing statement
    """

    # add output converters
    connection = prepare_connection(connection)

    # create cursor to fetch data
    cursor = connection.cursor()

    # read data from SQL
    if args is None:
        result = cursor.execute(statement).fetchall()
    else:
        result = cursor.execute(statement, *args).fetchall()
    columns = pd.Series([col[0] for col in cursor.description])

    # form output using SQL schema and explicit pandas types
    if any(~columns.isin(schema.index)):
        columns = list(columns[~columns.isin(schema.index)])
        raise AttributeError(f"missing columns from schema: {columns}")
    dtypes = schema.loc[columns, "pandas_type"].to_dict()
    result = {col: [row[idx] for row in result] for idx, col in enumerate(columns)}
    result = {col: pd.Series(vals, dtype=dtypes[col]) for col, vals in result.items()}
    result = pd.DataFrame(result)

    # set primary key columns as index
    keys = list(schema[schema["pk_seq"].notna()].index)
    if keys:
        try:
            result = result.set_index(keys=keys)
        except KeyError:
            raise KeyError(f"primary key column missing from query: {keys}")

    return result
