# import pandas as pd

# from mssql_dataframe import connect, collection

# db = connect.connect()
# sql = collection.SQLServer(db, adjust_sql_objects=True)

# df = pd.read_csv('C:/Users/jacoo/Desktop/Temp/test.csv')
# sql.write.merge('##NoTable', df)

import pyodbc

connection = pyodbc.connect(
    driver='ODBC Driver 17 for SQL Server', server='localhost', database='master',
    autocommit=False, trusted_connection='yes'
)
cursor = connection.cursor()
cursor.fast_executemany = True
cursor.execute("""
CREATE TABLE ##TableA(
    ColumnA VARCHAR(256),
    ColumnB TINYINT
)
""")

statement = """
INSERT INTO
##TableA (
    ColumnA, ColumnB
) VALUES (
    ?, ?
)
"""

args = [['a'*256, 1]]
# cursor.executemany(statement, args)
# cursor.commit()
# cursor.execute('SELECT * FROM ##TableA').fetchall()

cursor.setinputsizes([
    (pyodbc.SQL_WLONGVARCHAR,256,0),
    (pyodbc.SQL_VARCHAR,256,0)
])

cursor.executemany(statement, args)

# # expected behavior
# cursor.fast_executemany = True
# sz = 255
# args = [['a'*sz]]
# try:
#     cursor.executemany(statement, args)
# except pyodbc.ProgrammingError as err:
#     # True
#     print('Invalid object name' in str(err))

# # unexpected behavior
# cursor.fast_executemany = True
# sz = 256
# args = [['a'*256]]
# try:
#     cursor.executemany(statement, args)
# except pyodbc.ProgrammingError as err:
#     # False, err = ProgrammingError('String data, right truncation: length 512 buffer 510', 'HY000')
#     print('Invalid object name' in str(err))

# # work-around: fast_executemany=False
# cursor.fast_executemany = False
# sz = 256
# args = [['a'*sz]]
# try:
#     cursor.executemany(statement, args)
# except pyodbc.ProgrammingError as err:
#     # True
#     print('Invalid object name' in str(err))

# # work-around: setinputsize
# cursor.fast_executemany = True
# sz = 256
# args = [['a'*sz]]
# cursor.setinputsizes([(pyodbc.SQL_VARCHAR,sz*2,0)])
# try:
#     cursor.executemany(statement, args)
# except pyodbc.ProgrammingError as err:
#     # True
#     print('Invalid object name' in str(err))
# cursor.setinputsizes([
#     (pyodbc.SQL_VARCHAR,512,0),           # variable length string
#     (pyodbc.SQL_WLONGVARCHAR,512,0),      # unicode variable length character data
#     (pyodbc.SQL_WVARCHAR,512,0)           # unicode variable length character string
# ])