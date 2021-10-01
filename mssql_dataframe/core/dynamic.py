"""Functions for handling strings that include SQL objects."""

import re
from typing import Tuple, List

import pyodbc

from mssql_dataframe.core import errors


def escape(cursor: pyodbc.connect, inputs: List[str]) -> List[str]:
    """Prepare dynamic strings by passing them through T-SQL QUOTENAME.

    Parameters
    ----------
    cursor (pyodbc.connection.cursor) : cursor to execute statement
    inputs (list|str) : list of strings to add delimiter to make a valid SQL identifier

    Returns
    -------
    safe (list|str) : strings wrapped in SQL QUOTENAME

    """

    # handle both flat strings collection like inputs
    flatten = False
    if isinstance(inputs, str):
        flatten = True
        inputs = [inputs]
    elif not isinstance(inputs, list):
        inputs = list(inputs)

    # handle schema dot (.) specification that can seperate strings that need to be escaped
    # flatten each list and combine with the char(255) for a unique delimiter
    schema = [re.findall(r"\.+", x) for x in inputs]
    schema = [x + [chr(255)] for x in schema]
    schema = [item for sublist in schema for item in sublist]
    inputs = [re.split(r"\.+", x) for x in inputs]
    inputs = [item for sublist in inputs for item in sublist]

    # use QUOTENAME for each string
    statement = "SELECT {syntax}"
    syntax = ", ".join(["QUOTENAME(?)"] * len(inputs))
    statement = statement.format(syntax=syntax)
    cursor.execute(statement, *inputs)
    safe = cursor.fetchone()
    # a string value is too long and returns None, so raise an exception
    if [x for x in safe if x is None]:
        raise errors.SQLInvalidLengthObjectName("SQL object name is too long.")

    # reconstruct schema specification previously delimited by char(255)
    safe = list(zip(safe, schema))
    safe = [item for sublist in safe for item in sublist]
    safe = "".join(safe[0:-1]).split(chr(255))

    # return string if string was input
    if flatten:
        safe = safe[0]

    return safe


def where(cursor: pyodbc.connect, where: str) -> Tuple[str, list[str]]:
    """Format a raw string into a valid where statement with placeholder arguments.

    Parameters
    ----------
    cursor (pyodbc.connection.cursor) : cursor to execute statement
    where (str) : raw string to format

    Returns
    -------
    statement (str) : where statement containing parameters such as "...WHERE [username] = ?"
    args (list) : parameter values for where statement
    """

    # regular expressions to parse where statement
    combine = r"\bAND\b|\bOR\b"
    comparison = [
        "=",
        ">",
        "<",
        ">=",
        "<=",
        "<>",
        "!=",
        "!>",
        "!<",
        "IS NULL",
        "IS NOT NULL",
    ]
    comparison = r"(" + "|".join([x for x in comparison]) + ")"

    # split on AND/OR
    conditions = re.split(combine, where, flags=re.IGNORECASE)
    # split on comparison operator
    conditions = [re.split(comparison, x, flags=re.IGNORECASE) for x in conditions]
    if len(conditions) == 1 and len(conditions[0]) == 1:
        raise errors.SQLInvalidSyntax("invalid syntax for where = " + where)
    # form dict for each colum, while handling IS NULL/IS NOT NULL split
    conditions = [[y.strip() for y in x] for x in conditions]
    conditions = {x[0]: (x[1::] if len(x[2]) > 0 else [x[1]]) for x in conditions}

    # santize column names
    column_names = escape(cursor, conditions.keys())
    column_names = dict(zip(conditions.keys(), column_names))
    conditions = dict((column_names[key], value) for (key, value) in conditions.items())
    conditions = conditions.items()

    # form SQL where statement
    statement = [
        x[0] + " " + x[1][0] + " ?" if len(x[1]) > 1 else x[0] + " " + x[1][0]
        for x in conditions
    ]
    recombine = re.findall(combine, where, flags=re.IGNORECASE) + [""]
    statement = list(zip(statement, recombine))
    statement = "WHERE " + " ".join([x[0] + " " + x[1] for x in statement])
    statement = statement.strip()

    # form arguments, skipping IS NULL/IS NOT NULL
    args = {
        "param" + str(idx): x[1][1] for idx, x in enumerate(conditions) if len(x[1]) > 1
    }
    args = [x[1][1] for x in conditions if len(x[1]) > 1]

    return statement, args


def column_spec(columns: List[str]) -> List[str]:
    """Extract SQL data type, size, and precision from list of strings.

    Parameters
    ----------
    columns (list) : strings to extract SQL specifications from

    Returns
    -------
    size (list) : size of the SQL column
    dtypes_sql (list) : data type of the SQL column

    """

    flatten = False
    if isinstance(columns, str):
        columns = [columns]
        flatten = True

    pattern = r"(\(\d+\)|\(\d.+\)|\(MAX\))"
    size = [re.findall(pattern, x) for x in columns]
    size = [x[0] if len(x) > 0 else None for x in size]
    dtypes_sql = [re.sub(pattern, "", var) for var in columns]

    if flatten:
        size = size[0]
        dtypes_sql = dtypes_sql[0]

    return size, dtypes_sql
