''' Sample dataframe for testing.

key:  SQL data type
---
SQL data type with underscore prefixed

value: pd.Series([LowerLimit, UpperLimit, NULL, Truncation])
-----
LowerLimit: SQL lower limit or pandas lower limit if it is more restrictive
UpperLimit: SQL upper limit or pandas upper limit if it is more restrictive
NULL: SQL NULL / pandas <NA>
Truncation: truncated values due to SQL precision limit
'''

import pandas as pd

dataframe = pd.DataFrame({
    '_bit': pd.Series([False, True, None, None], dtype='boolean'),
    '_tinyint': pd.Series([0, 255, None, None], dtype='UInt8'),
    '_smallint': pd.Series([-2**15, 2**15-1, None, None], dtype='Int16'),
    '_int': pd.Series([-2**31, 2**31-1, None, None], dtype='Int32'),
    '_bigint': pd.Series([-2**63, 2**63-1, None, None], dtype='Int64'),
    '_float': pd.Series([-1.79**308, 1.79**308, None, None], dtype='float'),
    '_time': pd.Series(['00:00:00.0000000', '23:59:59.9999999', None, '00:00:01.123456789'], dtype='timedelta64[ns]'),
    '_date': pd.Series([(pd.Timestamp.min+pd.DateOffset(days=1)).date(), pd.Timestamp.max.date(), None, None], dtype='datetime64[ns]'),
    '_datetime2': pd.Series([pd.Timestamp.min, pd.Timestamp.max, None, pd.Timestamp('1970-01-01 00:00:01.123456789')], dtype='datetime64[ns]'),
    '_varchar': pd.Series(['a', 'bbb', None], dtype='string'),
    '_nvarchar': pd.Series([u'100\N{DEGREE SIGN}F', u'company name\N{REGISTERED SIGN}', None], dtype='string'),
})