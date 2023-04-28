import env

import pytest
import pandas as pd

from mssql_dataframe.core import custom_errors

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create
from mssql_dataframe.core.write import insert, update, merge


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.insert = insert.insert(self.connection)
        self.update = update.update(self.connection)
        self.merge = merge.merge(self.connection)


@pytest.fixture(scope="module")
def sql():
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    yield package(db)
    db.connection.close()


def test_insert_error_nonexistant(sql):
    table_name = "##test_insert_error_nonexistant"

    sql.create.table(
        table_name, columns={"ColumnB": "SMALLINT", "ColumnC": "VARCHAR(1)"}
    )

    with pytest.raises(custom_errors.SQLColumnDoesNotExist):
        dataframe = pd.DataFrame({"ColumnA": [1]})
        sql.insert.insert(table_name, dataframe=dataframe)

    with pytest.raises(custom_errors.SQLTableDoesNotExist):
        dataframe = pd.DataFrame({"ColumnB": [1]})
        sql.insert.insert("##error" + table_name, dataframe=dataframe)


def test_insert_error_insufficent(sql):
    table_name = "##test_insert_error_insufficent"

    sql.create.table(
        table_name,
        columns={
            "_smallint": "SMALLINT",
            "_char": "CHAR(1)",
            "_nchar": "NCHAR(1)",
            "_varchar": "VARCHAR(1)",
            "_nvarchar": "NVARCHAR(1)",
        },
    )

    with pytest.raises(custom_errors.SQLInsufficientColumnSize):
        sql.insert.insert(table_name, dataframe=pd.DataFrame({"_smallint": [100000]}))

    dtypes = {"_char": "a", "_varchar": "a", "_nchar": "え", "_nvarchar": "え"}
    for col, val in dtypes.items():
        with pytest.raises(custom_errors.SQLInsufficientColumnSize):
            dataframe = pd.DataFrame({col: [val * 3]})
            sql.insert.insert(table_name, dataframe=dataframe)


def test_unicode_error(sql):
    table_name = "##test_unicode_error"

    sql.create.table(table_name, columns={"_char": "CHAR(1)", "_varchar": "VARCHAR(1)"})

    dtypes = {"_char": "え", "_varchar": "え"}
    for col, val in dtypes.items():
        with pytest.raises(custom_errors.SQLNonUnicodeTypeColumn):
            dataframe = pd.DataFrame({col: [val]})
            sql.insert.insert(table_name, dataframe=dataframe)


def test_update_errors(sql):
    table_name = "##test_update_errors"
    sql.create.table(
        table_name, columns={"ColumnA": "TINYINT", "ColumnB": "VARCHAR(1)"}
    )

    with pytest.raises(custom_errors.SQLTableDoesNotExist):
        sql.update.update(
            "error" + table_name, dataframe=pd.DataFrame({"ColumnA": [1]})
        )

    with pytest.raises(custom_errors.SQLColumnDoesNotExist):
        sql.update.update(
            table_name,
            dataframe=pd.DataFrame({"ColumnA": [0], "ColumnC": [1]}),
            match_columns=["ColumnA"],
        )

    with pytest.raises(custom_errors.SQLInsufficientColumnSize):
        sql.update.update(
            table_name,
            dataframe=pd.DataFrame({"ColumnA": [100000], "ColumnB": ["aaa"]}),
            match_columns=["ColumnA"],
        )

    with pytest.raises(custom_errors.SQLUndefinedPrimaryKey):
        sql.update.update(
            table_name, dataframe=pd.DataFrame({"ColumnA": [1], "ColumnB": ["a"]})
        )

    with pytest.raises(custom_errors.SQLColumnDoesNotExist):
        sql.update.update(
            table_name,
            dataframe=pd.DataFrame({"ColumnA": [1], "ColumnB": ["a"], "ColumnC": [1]}),
            match_columns=["ColumnC"],
        )

    with pytest.raises(custom_errors.DataframeColumnDoesNotExist):
        sql.update.update(
            table_name,
            dataframe=pd.DataFrame({"ColumnA": [1]}),
            match_columns=["ColumnB"],
        )


def test_merge_errors(sql):
    table_name = "##test_merge_errors"
    sql.create.table(
        table_name, columns={"ColumnA": "TINYINT", "ColumnB": "VARCHAR(1)"}
    )

    with pytest.raises(custom_errors.SQLTableDoesNotExist):
        sql.merge.merge("error" + table_name, dataframe=pd.DataFrame({"ColumnA": [1]}))

    with pytest.raises(custom_errors.SQLColumnDoesNotExist):
        sql.merge.merge(
            table_name,
            dataframe=pd.DataFrame({"ColumnA": [0], "ColumnC": [1]}),
            match_columns=["ColumnA"],
        )

    with pytest.raises(custom_errors.SQLInsufficientColumnSize):
        sql.merge.merge(
            table_name,
            dataframe=pd.DataFrame({"ColumnA": [100000], "ColumnB": ["aaa"]}),
            match_columns=["ColumnA"],
        )

    with pytest.raises(ValueError):
        sql.merge.merge(
            table_name,
            dataframe=pd.DataFrame({"ColumnA": [100000], "ColumnB": ["aaa"]}),
            upsert=True,
            delete_requires=["ColumnB"],
        )
