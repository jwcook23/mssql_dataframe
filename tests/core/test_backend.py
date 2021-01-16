# import mssql_dataframe

def test_answer():
    assert 5 == 5

# import pandas as pd
# import sqlalchemy as sql


# def test_SQLServer():
#     db = SQLServer('AdventureWorks2019')

# test_SQLServer()

# dataframe = pd.DataFrame([
#     [1, 'name1', 'add1', 11],
#     [2, 'name2', 'add2', 12],
#     [3, 'name3', 'add3', 13],
#     [4, 'name4', 'add4', 14],
# ],columns = ['RowID','Name','Address','Value'])


# # Main Table
# table = sql.Table('_test', db.metadata, 
#     sql.Column('RowID', sql.NVARCHAR(None)),
#     sql.Column('Name', sql.NVARCHAR(None)),
#     sql.Column('Address', sql.NVARCHAR(None)),
#     sql.Column('Value', sql.NVARCHAR(None)),
#     schema='dbo')
# table.drop(db.engine, checkfirst=True)
# table.create(db.engine)

# connection = db.engine.connect()
# connection.execute(table.insert(), [
#     {'RowID':1, 'Name':'name1', 'Address':'add1', 'Value':11},
#     {'RowID':2, 'Name':'name2', 'Address':'add2', 'Value':12},
#     {'RowID':3, 'Name':'name3', 'Address':'add3', 'Value':13},
#     {'RowID':4, 'Name':'name4', 'Address':'add4', 'Value':14},
# ])

# table = db.Table('dbo.DatabaseLog',metadata)

# table.columns.constraints