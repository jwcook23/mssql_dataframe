import pandas as pd

from mssql_dataframe import connect, collection

db = connect.connect()
sql = collection.SQLServer(db, adjust_sql_objects=True)

dataframe = pd.read_csv('test.csv')
dataframe = dataframe[dataframe.columns[1::]]

table_name = '##Test'
dataframe['URL'] = dataframe['URL'].str.slice(0,256) #671
sql.write.merge(table_name, dataframe=dataframe.head(3))