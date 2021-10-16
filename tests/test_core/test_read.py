import warnings

import pytest
import pandas as pd

pd.options.mode.chained_assignment = "raise"

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_errors, create, read
from mssql_dataframe.core.write import insert

table_name = "##test_select"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.insert = insert.insert(self.connection, include_metadata_timestamps=False, autoadjust_sql_objects=False)
        self.read = read.read(self.connection)


@pytest.fixture(scope="session")
def sql():
    db = connect(database="tempdb", server="localhost")
    yield package(db)
    db.connection.close()


@pytest.fixture(scope="session")
def sample(sql):

    # create table and insert sample data
    sql.create.table(
        table_name,
        columns={
            "ColumnA": "TINYINT",
            "ColumnB": "INT",
            "ColumnC": "BIGINT",
            "ColumnD": "DATE",
            "ColumnE": "VARCHAR(1)",
            "ColumnF": "VARCHAR(3)",
        },
        primary_key_column=["ColumnA", "ColumnF"],
    )

    dataframe = pd.DataFrame(
        {
            "ColumnA": [5, 6, 7],
            "ColumnB": [5, 6, None],
            "ColumnC": [pd.NA, 6, 7],
            "ColumnD": ["06-22-2021", "06-22-2021", pd.NaT],
            "ColumnE": ["a", "b", None],
            "ColumnF": ["xxx", "yyy", "zzz"],
        }
    ).set_index(keys=["ColumnA", "ColumnF"])
    dataframe["ColumnB"] = dataframe["ColumnB"].astype("Int64")
    dataframe["ColumnD"] = pd.to_datetime(dataframe["ColumnD"])
    dataframe = sql.insert.insert(table_name, dataframe)

    yield dataframe


def test_select_errors(sql, sample):

    table_name = "##test_select_errors"
    sql.create.table(table_name, columns={"ColumnA": "TINYINT"})

    with pytest.raises(ValueError):
        sql.read.table(table_name, limit="1")

    with pytest.raises(ValueError):
        sql.read.table(table_name, order_column="A", order_direction=None)

    with pytest.raises(ValueError):
        sql.read.table(table_name, order_column="A", order_direction="a")

    # sample is needed to create the SQL table
    assert isinstance(sample, pd.DataFrame)
    with pytest.raises(custom_errors.SQLColumnDoesNotExist):
        sql.read.table(table_name, column_names="NotAColumn")


def test_undefined_conversion(sql):

    table_name = "##test_undefined_conversion"
    columns = {"_geography": "GEOGRAPHY", "_datetimeoffset": "DATETIMEOFFSET(4)"}
    sql.create.table(table_name, columns)

    geography = "geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656)', 4326)"
    datetimeoffset = "'12-10-25 12:32:10 +01:00'"
    statement = "INSERT INTO {table_name} VALUES({geography},{datetimeoffset})"
    cursor = sql.connection.cursor()
    cursor.execute(
        statement.format(
            table_name=table_name, geography=geography, datetimeoffset=datetimeoffset
        )
    )
    cursor.commit()

    with pytest.raises(custom_errors.UndefinedConversionRule):
        sql.read.table(table_name)


def test_select_all(sql, sample):

    dataframe = sql.read.table(table_name)
    assert dataframe.equals(sample)


def test_select_columns(sql, sample):

    column_names = sample.columns.drop("ColumnB")
    dataframe = sql.read.table(table_name, column_names)
    assert dataframe[column_names].equals(sample[column_names])

    column_names = "ColumnB"
    dataframe = sql.read.table(table_name, column_names)
    assert dataframe[[column_names]].equals(sample[[column_names]])


def test_select_where(sql, sample):

    column_names = ["ColumnB", "ColumnC", "ColumnD"]
    dataframe = sql.read.table(
        table_name,
        column_names,
        where="ColumnB>4 AND ColumnC IS NOT NULL OR ColumnD IS NULL",
    )
    query = "(ColumnB>5 and ColumnC.notnull()) or ColumnD.isnull()"
    assert all(dataframe.columns.isin(column_names))
    assert dataframe.equals(sample[dataframe.columns].query(query))


def test_select_limit(sql, sample):

    dataframe = sql.read.table(table_name, limit=1)
    assert dataframe.shape[0] == 1
    assert dataframe.equals(sample.loc[[dataframe.index[0]]])


def test_select_order(sql, sample):

    dataframe = sql.read.table(
        table_name,
        column_names=["ColumnB"],
        order_column="ColumnA",
        order_direction="DESC",
    )
    assert dataframe.equals(
        sample[["ColumnB"]].sort_values(
            by="ColumnB", ascending=False, na_position="first"
        )
    )
