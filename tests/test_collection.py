import warnings

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors

def test_SQLServer():
    
    db = connect()

    sql = SQLServer(db, adjust_sql_objects=False)
    assert isinstance(sql, SQLServer)

    with warnings.catch_warnings(record=True) as warn:
        sql_adjustable = SQLServer(db, adjust_sql_objects=True)
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert isinstance(sql_adjustable, SQLServer)