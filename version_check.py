import sys
from mssql_dataframe import SQLServer
sql = SQLServer(server=r"(localdb)\mssqllocaldb")
sys.stdout.write("Connection: \n"+str(sql._conn)+"\n")
sys.stdout.write("Versions: \n"+str(sql._versions)+"\n")