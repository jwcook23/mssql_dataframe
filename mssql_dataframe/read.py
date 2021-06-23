from typing import Literal

import pandas as pd

from mssql_dataframe import errors
from mssql_dataframe import helpers


def select(connection, table_name: str, column_names: list, conditions) -> pd.DataFrame:
    """Select data from SQL into a dataframe.

    Parameters
    ----------

    connection (mssql_dataframe.connect) : connection for executing statement
    table_name (str) : name of table to select data frame
    column_names (list) : list of columns to select

    Returns
    -------

    dataframe (pd.DataFrame): tabular data from select statement
    
    None

    Examples
    --------



    """

    # sanitize table and column names for safe sql
    table_name = helpers.safe_sql(connection, table_name)
    table_name = table_name[0]
    column_names = helpers.safe_sql(connection, column_names)
    column_names = ", ".join(column_names)

    # cursor.execute("SELECT admin FROM users WHERE username = %s'", (username, ));
    # cursor.execute("SELECT admin FROM users WHERE username = %(username)s", {'username': username});

    # =, >, <, >=, <=, <>, !=, !>, !<, IS NULL, IS NOT NULL

    # select values
    statement = """
    SELECT {limit}
        {column_names}
    FROM
        {table_name}
    WHERE
        {conditions}
    """

    params = '('+', '.join(['?']*len(dataframe.columns))+')'
    statement = "SELECT "+column_names+" FROM "+table_name
    dataframe = __prepare_values(dataframe)
    values = dataframe.values.tolist()
    try:
        connection.cursor.executemany(statement, values)
    except:
        raise errors.GeneralError("GeneralError")