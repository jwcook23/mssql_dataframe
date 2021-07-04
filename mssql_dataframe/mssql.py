from mssql_dataframe import(
    connect,
    create,
    modify,
    read,
    write
)

class server():

    def __init__(self):

        self.connection = connect.SQLServer(database_name='master', server_name='localhost')
        self.create = create.create(self.connection)
        self.modify = modify.modify(self.connection)
        self.read = read.read(self.connection)
        self.write = write.write(self.connection)

sql = server()
# sql.create.from_dataframe()