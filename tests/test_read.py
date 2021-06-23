import pytest
import pandas as pd
import numpy as np
from datetime import date

from mssql_dataframe import connect
from mssql_dataframe import write
from mssql_dataframe import read

@pytest.fixture(scope="module")
def connection():

    db = connect.SQLServer(database_name='tempdb', server_name='localhost', autocommit=False)
    yield db
    db.connection.close()


def test_select(connection):

    pass