import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.collection import SQLServer

db = connect(database_name='master', server_name='localhost')
# # initialize the main package
sql = SQLServer(db, adjust_sql_objects=True)

dataframe = pd.read_csv(r'C:\Users\jacoo\Desktop\Temp\test.csv')
dataframe = dataframe[dataframe.columns[1::]]


sql.write.merge('##test_error', dataframe)

print('2')