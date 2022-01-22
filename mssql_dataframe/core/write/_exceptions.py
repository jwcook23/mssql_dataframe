"""Functions for handling exceptions when attempting to write to SQL."""
import warnings
from typing import List

import pandas as pd

from mssql_dataframe.core import (
    custom_warnings,
    custom_errors,
    infer,
    conversion,
    modify,
    create,
)


def handle(
    failure: custom_errors,
    table_name: str,
    dataframe: pd.DataFrame,
    updating_table: bool,
    autoadjust_sql_objects: bool,
    modifier: modify,
    creator: create,
) -> pd.DataFrame:
    """Handle a failed write attempt.

    Parameters
    ----------
    failure (mssql_dataframe.core.errors) : exception to potentially handle
    table_name (str) : name of the table for which the failed write attempt occured
    dataframe (pandas.DataFrame) : data to insert
    updating_table (bool, default) : flag that indicates if target table is being updated
    autoadjust_sql_objects (bool) : flag for if tables will be created or objects with be modified
    modifier (mssql_dataframe.core.modify) : class to modify SQL columns
    creator (mssql_dataframe.core.create) : class to create SQL tables

    Returns
    -------
    dataframe (pandas.DataFrame) : data to insert that may have been adjust to conform to SQL data types

    """
    # check if specific columns initiated the failure
    if len(failure.args) > 1:
        columns = pd.Series(failure.args[1], dtype="string")
    else:
        columns = pd.Series([], dtype="string")

    # always add include_metadata_timestamps columns, regardless of autoadjust_sql_objects value
    include_metadata_timestamps = ["_time_insert", "_time_update"]
    if isinstance(failure, custom_errors.SQLColumnDoesNotExist) and all(
        columns.isin(include_metadata_timestamps)
    ):
        for col in columns:
            warnings.warn(
                f"Creating column {col} in table {table_name} with data type DATETIME2.",
                custom_warnings.SQLObjectAdjustment,
            )
            modifier.column(
                table_name, modify="add", column_name=col, data_type="DATETIME2"
            )

    elif not autoadjust_sql_objects:
        raise failure

    elif isinstance(failure, custom_errors.SQLTableDoesNotExist):
        if updating_table:
            raise failure
        else:
            dataframe = create_table(table_name, dataframe, creator)

    elif isinstance(failure, custom_errors.SQLColumnDoesNotExist):
        dataframe = add_columns(table_name, dataframe, columns, modifier)

    elif isinstance(failure, custom_errors.SQLInsufficientColumnSize):
        dataframe = alter_columns(table_name, dataframe, columns, modifier)

    else:
        raise failure

    return dataframe


def create_table(
    table_name: str, dataframe: pd.DataFrame, creator: modify
) -> pd.DataFrame:
    """Create a table if it does not exist.

    Parameters
    ----------
    table_name (str) : name of the table for which the failed write attempt occured
    dataframe (pandas.DataFrame) : data to insert
    creator (mssql_dataframe.core.create) : class to create SQL tables

    Returns
    -------
    dataframe (pandas.DataFrame) : data to insert that may have been adjust to conform to SQL data types

    """
    warnings.warn(
        "Creating table {}".format(table_name), custom_warnings.SQLObjectAdjustment
    )

    if any(dataframe.index.names):
        primary_key = "index"
    else:
        primary_key = "infer"

    creator.table_from_dataframe(
        table_name, dataframe, primary_key=primary_key, insert_dataframe=False
    )

    return dataframe


def add_columns(
    table_name: str, dataframe: pd.DataFrame, columns: List[str], modifier: modify
) -> pd.DataFrame:
    """Add columns if they do not exist.

    Parameters
    ----------
    table_name (str) : name of the table for which the failed write attempt occured
    dataframe (pandas.DataFrame) : data to insert
    columns (list) : columns to add
    modifier (mssql_dataframe.core.modify) : class to modify SQL columns

    Returns
    -------
    dataframe (pandas.DataFrame) : data to insert that may have been adjust to conform to SQL data types
    """

    # infer the data types for new columns
    new, schema, _, _ = infer.sql(dataframe.loc[:, columns])
    # determine the SQL data type for each column
    _, dtypes = conversion.sql_spec(schema, new)
    # add each column
    for col, spec in dtypes.items():
        warnings.warn(
            f"Creating column {col} in table {table_name} with data type {spec}.",
            custom_warnings.SQLObjectAdjustment,
        )
        modifier.column(
            table_name, modify="add", column_name=col, data_type=spec, is_nullable=True
        )
    # add potentially adjusted columns back into dataframe
    dataframe[new.columns] = new

    return dataframe


def alter_columns(
    table_name: str, dataframe: pd.DataFrame, columns: List[str], modifier: modify
) -> pd.DataFrame:
    """Alter columns if their size needs to be increased.

    Parameters
    ----------
    table_name (str) : name of the table for which the failed write attempt occured
    dataframe (pandas.DataFrame) : data to insert
    columns (list) : columns to alter
    modifier (mssql_dataframe.core.modify) : class to modify SQL columns

    Returns
    -------
    dataframe (pandas.DataFrame) : data to insert that may have been adjust to conform to SQL data types
    """

    # temporarily set named index (primary key) as columns
    index = dataframe.index.names
    if any(index):
        dataframe = dataframe.reset_index()
    # infer the data types for insufficient size columns
    new, schema, _, _ = infer.sql(dataframe.loc[:, columns])
    schema, dtypes = conversion.sql_spec(schema, new)
    # get current table schema
    previous, _ = conversion.get_schema(modifier._connection, table_name)
    strings = previous["sql_type"].isin(["varchar", "nvarchar"])
    previous.loc[strings, "odbc_size"] = previous.loc[strings, "column_size"]
    # insure change within the same sql data type category after inferring dtypes
    unchanged = (
        previous.loc[schema.index, ["sql_type", "odbc_size"]]
        == schema[["sql_type", "odbc_size"]]
    )
    unchanged = unchanged.all(axis="columns")
    if any(unchanged):
        unchanged = list(unchanged[unchanged].index)
        raise custom_errors.SQLRecastColumnUnchanged(
            f"Handling SQLInsufficientColumnSize did not result in type or size change for columns: {unchanged}"
        )
    # insure change doesn't result in different sql data category
    changed = previous.loc[schema.index, ["sql_category"]] != schema[["sql_category"]]
    if any(changed["sql_category"]):
        changed = list(changed[changed["sql_category"]].index)
        raise custom_errors.DataframeColumnInvalidValue(
            "Dataframe columns cannot be converted based on their SQL data type",
            changed,
        )
    # drop primary key constraint prior to altering columns, if needed
    primary_key_columns = (
        previous.loc[previous["pk_seq"].notna(), "pk_seq"]
        .sort_values(ascending=True)
        .index
    )
    if len(primary_key_columns) == 0:
        primary_key_name = None
    else:
        primary_key_name = previous.loc[primary_key_columns[0], "pk_name"]
        modifier.primary_key(
            table_name,
            modify="drop",
            columns=primary_key_columns,
            primary_key_name=primary_key_name,
        )
    # alter each column
    for col, spec in dtypes.items():
        is_nullable = previous.at[col, "is_nullable"]
        warnings.warn(
            f"Altering column {col} in table {table_name} to data type {spec} with is_nullable={is_nullable}.",
            custom_warnings.SQLObjectAdjustment,
        )
        modifier.column(
            table_name,
            modify="alter",
            column_name=col,
            data_type=spec,
            is_nullable=is_nullable,
        )
    # readd primary key if needed
    if primary_key_name:
        modifier.primary_key(
            table_name,
            modify="add",
            columns=list(primary_key_columns),
            primary_key_name=primary_key_name,
        )
    # reset primary key columns as dataframe's index
    if any(index):
        dataframe = dataframe.set_index(keys=index)

    return dataframe
