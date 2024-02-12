"""Functions for handling exceptions when attempting to write to SQL."""

import logging

import pandas as pd

from mssql_dataframe.core import (
    custom_errors,
    modify,
)

logger = logging.getLogger(__name__)


def add_metadata_timestamps(
    failure: custom_errors,
    table_name: str,
    dataframe: pd.DataFrame,
    modifier: modify,
) -> pd.DataFrame:
    """Handle a failed write attempt.

    Parameters
    ----------
    failure (mssql_dataframe.core.errors) : exception to potentially handle
    table_name (str) : name of the table for which the failed write attempt occured
    dataframe (pandas.DataFrame) : data to insert
    modifier (mssql_dataframe.core.modify) : class to modify SQL columns

    Returns
    -------
    dataframe (pandas.DataFrame) : data to insert that may have been adjust to conform to SQL data types

    """
    columns = pd.Series(failure.args[1], dtype="string")
    include_metadata_timestamps = ["_time_insert", "_time_update"]
    if isinstance(failure, custom_errors.SQLColumnDoesNotExist) and all(
        columns.isin(include_metadata_timestamps)
    ):
        for col in columns:
            msg = f"Creating column '{col}' in table '{table_name}' with data type 'datetime2'."
            logger.warning(msg)
            modifier.column(
                table_name, modify="add", column_name=col, data_type="datetime2"
            )

    else:
        raise failure

    return dataframe
