import pyodbc
import pandas as pd

# relationship between SQL, pandas, and ODBC data types
relation = pd.DataFrame.from_records([
    {'sql_type': 'bit', 'pandas_type': 'boolean', 'odbc_type': pyodbc.SQL_BIT},
    {'sql_type': 'tinyint', 'pandas_type': 'UInt8', 'odbc_type': pyodbc.SQL_TINYINT},
    {'sql_type': 'smallint', 'pandas_type': 'Int16', 'odbc_type': pyodbc.SQL_SMALLINT},
    {'sql_type': 'int', 'pandas_type': 'Int32', 'odbc_type': pyodbc.SQL_INTEGER},
    {'sql_type': 'bigint', 'pandas_type': 'Int64', 'odbc_type': pyodbc.SQL_BIGINT},
    {'sql_type': 'float', 'pandas_type': 'float64', 'odbc_type': pyodbc.SQL_FLOAT},
    {'sql_type': 'decimal', 'pandas_type': 'float64', 'odbc_type': pyodbc.SQL_DECIMAL},
    {'sql_type': 'time', 'pandas_type': 'timedelta64[ns]', 'odbc_type': pyodbc.SQL_TYPE_TIME},
    {'sql_type': 'date', 'pandas_type': 'datetime64[ns]', 'odbc_type': pyodbc.SQL_TYPE_DATE},
    {'sql_type': 'datetime2', 'pandas_type': 'datetime64[ns]', 'odbc_type': pyodbc.SQL_TYPE_TIMESTAMP},
    {'sql_type': 'varchar', 'pandas_type': 'string', 'odbc_type': pyodbc.SQL_VARCHAR},
    {'sql_type': 'nvarchar', 'pandas_type': 'string', 'odbc_type': pyodbc.SQL_WVARCHAR},
])

# 


# TODO: SQL cross apply statement to infer data types
# infer = """
# (CASE 
#     WHEN count(try_convert(BIT, _Column)) = count(_Column) 
#         AND MAX(_Column)=1 AND count(_Column)>2 THEN ''bit''
#     WHEN count(try_convert(TINYINT, _Column)) = count(_Column) THEN ''tinyint''
#     WHEN count(try_convert(SMALLINT, _Column)) = count(_Column) THEN ''smallint''
#     WHEN count(try_convert(INT, _Column)) = count(_Column) THEN ''int''
#     WHEN count(try_convert(BIGINT, _Column)) = count(_Column) THEN ''bigint''
#     WHEN count(try_convert(TIME, _Column)) = count(_Column) 
#         AND SUM(CASE WHEN try_convert(DATE, _Column) = ''1900-01-01'' THEN 0 ELSE 1 END) = 0
#         THEN ''time''
#     WHEN count(try_convert(DATETIME, _Column)) = count(_Column) THEN ''datetime''
#     WHEN count(try_convert(FLOAT, _Column)) = count(_Column) THEN ''float''
#     ELSE ''varchar''
# END) AS type
# """