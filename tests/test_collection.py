import warnings

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer
from mssql_dataframe.core import errors

attributes = ['connection','create','modify','read','insert','update','merge']

def test_SQLServer():
    
    # local host connection to database
    db = connect()

    # adjust_sql_objects==False
    with warnings.catch_warnings(record=True) as warn:
        assert len(warn)==0
        sql = SQLServer(db, adjust_sql_objects=False)
        assert isinstance(sql, SQLServer)
        assert all([hasattr(sql,x) for x in attributes])
        
    # adjust_sql_objects==True
    with warnings.catch_warnings(record=True) as warn:
        adjustable = SQLServer(db, adjust_sql_objects=True)
        assert len(warn)==1
        assert isinstance(warn[-1].message, errors.SQLObjectAdjustment)
        assert str(warn[0].message)=='SQL objects will be created/modified as needed as adjust_sql_objects=True'
        assert isinstance(adjustable, SQLServer)
        assert all([hasattr(adjustable,x) for x in attributes])