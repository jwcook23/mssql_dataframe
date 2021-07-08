from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

def test_SQLServer():
    
    db = connect()
    sql = SQLServer(db)
    assert isinstance(sql, SQLServer)