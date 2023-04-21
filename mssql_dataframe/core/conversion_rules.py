"""Rules for conversion between SQL, pandas, and odbc data types."""
import pandas as pd
from numpy import inf
import pyodbc

rules = pd.DataFrame.from_records(
    [
        {
            "sql_type": "bit",
            "sql_category": "boolean",
            "min_value": False,
            "max_value": True,
            "pandas_type": "boolean",
            "odbc_type": pyodbc.SQL_BIT
        },
        {
            "sql_type": "tinyint",
            "sql_category": "exact_whole_numeric",
            "min_value": 0,
            "max_value": 255,
            "pandas_type": "UInt8",
            "odbc_type": pyodbc.SQL_TINYINT
        },
        {
            "sql_type": "smallint",
            "sql_category": "exact_whole_numeric",
            "min_value": -(2**15),
            "max_value": 2**15 - 1,
            "pandas_type": "Int16",
            "odbc_type": pyodbc.SQL_SMALLINT
        },
        {
            "sql_type": "int",
            "sql_category": "exact_whole_numeric",
            "min_value": -(2**31),
            "max_value": 2**31 - 1,
            "pandas_type": "Int32",
            "odbc_type": pyodbc.SQL_INTEGER
        },
        {
            "sql_type": "bigint",
            "sql_category": "exact_whole_numeric",
            "min_value": -(2**63),
            "max_value": 2**63 - 1,
            "pandas_type": "Int64",
            "odbc_type": pyodbc.SQL_BIGINT
        },
        {
            "sql_type": "float",
            "sql_category": "approximate_decimal_numeric",
            "min_value": -(1.79**308),
            "max_value": 1.79**308,
            "pandas_type": "float64",
            "odbc_type": pyodbc.SQL_FLOAT
        },
        {
            "sql_type": "numeric",
            "sql_category": "exact_decimal_numeric",
            "min_value": -inf,
            "max_value": inf,
            "pandas_type": "object",
            "odbc_type": pyodbc.SQL_NUMERIC
        },
        {
            "sql_type": "decimal",
            "sql_category": "exact_decimal_numeric",
            "min_value": -inf,
            "max_value": inf,
            "pandas_type": "object",
            "odbc_type": pyodbc.SQL_DECIMAL
        },
        {
            "sql_type": "time",
            "sql_category": "date_time",
            "min_value": pd.Timedelta("00:00:00.0000000"),
            "max_value": pd.Timedelta("23:59:59.9999999"),
            "pandas_type": "timedelta64[ns]",
            "odbc_type": pyodbc.SQL_SS_TIME2
        },
        {
            "sql_type": "date",
            "sql_category": "date_time",
            "min_value": pd.Timestamp((pd.Timestamp.min + pd.Timedelta(days=1)).date()),
            "max_value": pd.Timestamp(pd.Timestamp.max.date()),
            "pandas_type": "datetime64[ns]",
            "odbc_type": pyodbc.SQL_TYPE_DATE
        },
        {
            "sql_type": "datetime",
            "sql_category": "date_time",
            "min_value": pd.Timestamp(1753,1,1,0,0,0),
            "max_value": pd.Timestamp(1900,1,1)+pd.Timedelta.max,
            "pandas_type": "datetime64[ns]",
            "odbc_type": pyodbc.SQL_TYPE_TIMESTAMP
        },
        {
            "sql_type": "datetimeoffset",
            "sql_category": "date_time",
            # TODO: inforce SQL TZ offset limit of -14:00 through +14:00
            "min_value": pd.Timestamp(pd.Timestamp.min, tz='UTC'),
            "max_value": pd.Timestamp(pd.Timestamp.max, tz='UTC'),
            "pandas_type": "object",
            "odbc_type": -155
        }, 
        {
            "sql_type": "datetime2",
            "sql_category": "date_time",
            "min_value": pd.Timestamp.min,
            "max_value": pd.Timestamp.max,
            "pandas_type": "datetime64[ns]",
            "odbc_type": pyodbc.SQL_TYPE_TIMESTAMP
        },
        {
            "sql_type": "char",
            "sql_category": "character string",
            "min_value": 1,
            "max_value": 0,
            "pandas_type": "string",
            "odbc_type": pyodbc.SQL_CHAR
        },
        {
            "sql_type": "varchar",
            "sql_category": "character string",
            "min_value": 1,
            "max_value": 0,
            "pandas_type": "string",
            "odbc_type": pyodbc.SQL_VARCHAR
        },
        {
            "sql_type": "nchar",
            "sql_category": "character string",
            "min_value": 1,
            "max_value": 0,
            "pandas_type": "string",
            "odbc_type": pyodbc.SQL_WCHAR
        },
        {
            "sql_type": "nvarchar",
            "sql_category": "character string",
            "min_value": 1,
            "max_value": 0,
            "pandas_type": "string",
            "odbc_type": pyodbc.SQL_WVARCHAR
        },
    ]
)
rules["sql_type"] = rules["sql_type"].astype("string")
rules["pandas_type"] = rules["pandas_type"].astype("string")
