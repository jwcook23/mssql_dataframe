import warnings

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors

def test_SQLServer():
    
    db = connect()

    sql = SQLServer(db, adjust_sql_objects=False)
    assert isinstance(sql, SQLServer)

    with warnings.catch_warnings(record=True) as warn:
        sql = SQLServer(db, adjust_sql_objects=True)
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert str(warn[0].message)=='SQL objects will be created/modified as needed as adjust_sql_objects=True'

    assert isinstance(sql, SQLServer)
    assert hasattr(sql,'connection')
    assert hasattr(sql,'create')
    assert hasattr(sql,'modify')
    assert hasattr(sql,'read')
    assert hasattr(sql,'insert')
    assert hasattr(sql,'update')
    assert hasattr(sql,'merge')