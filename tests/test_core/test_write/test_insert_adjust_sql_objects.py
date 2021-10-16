import warnings
from datetime import datetime

import pytest
import pandas as pd

pd.options.mode.chained_assignment = "raise"

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_warnings, custom_errors, create, conversion, conversion_rules
from mssql_dataframe.core.write import insert, _exceptions


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.create_meta = create.create(self.connection, include_metadata_timestamps=True)
        self.insert = insert.insert(self.connection, autoadjust_sql_objects=True)
        self.insert_meta = insert.insert(self.connection, include_metadata_timestamps=True, autoadjust_sql_objects=True)

@pytest.fixture(scope="module")
def sql():
    db = connect(database_name="tempdb", server_name="localhost")
    yield package(db)
    db.connection.close()

def test_insert_autoadjust_errors(sql):
    
    table_name = "##test_insert_autoadjust_errors"

    # create table with column for each conversion rule
    columns = conversion_rules.rules['sql_type'].to_numpy()
    columns = {'_'+x:x for x in columns}
    sql.create.table(table_name, columns=columns)

    # create dataframes for each conversion rule that should fail insert
    boolean = [3]
    exact_numeric = ['a', '2-1', 1.1, datetime.now()]
    approximate_numeric = ['a', '2-1',datetime.now()]
    date_time = ['a', 1, 1.1]
    character_string = [1, datetime.now()]
    dataframe = [
        pd.DataFrame({'_bit': boolean}),
        pd.DataFrame({'_tinyint': exact_numeric}),
        pd.DataFrame({'_smallint': exact_numeric}),
        pd.DataFrame({'_int': exact_numeric}),
        pd.DataFrame({'_bigint': exact_numeric}),
        pd.DataFrame({'_float': approximate_numeric}),
        pd.DataFrame({'_time': date_time}),
        pd.DataFrame({'_date': date_time}),
        pd.DataFrame({'_datetime2': date_time}),
        pd.DataFrame({'_varchar': character_string}),
        pd.DataFrame({'_nvarchar': character_string}),
    ]

    # insure all conversion rules are being tested
    assert pd.Series(columns.keys()).isin([x.columns[0] for x in dataframe]).all()

    for df in dataframe:
        # check each row to infer to base pandas type
        for row in df.index:
            with pytest.raises(custom_errors.DataframeColumnInvalidValue):
                sql.insert.insert(table_name, df.loc[[row]].infer_objects())


def test_insert_create_table(sql):

    table_name = "##test_insert_create_table"

    dataframe = pd.DataFrame(
        {"ColumnA": [1, 2, 3], "ColumnB": ["06/22/2021", "06-22-2021", "2021-06-22"]}
    )

    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.insert_meta.insert(
            table_name, dataframe=dataframe
        )
        assert len(warn) == 3
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert "Creating table " + table_name in str(warn[0].message)
        assert "Created table: " + table_name in str(warn[1].message)
        assert "Creating column _time_insert in table " + table_name in str(
            warn[2].message
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(f"SELECT * FROM {table_name}", schema, sql.connection)
    expected = pd.DataFrame(
        {
            "ColumnA": pd.Series([1, 2, 3], dtype="UInt8"),
            "ColumnB": pd.Series(
                [pd.Timestamp(year=2021, month=6, day=22)] * 3,
                dtype="datetime64[ns]",
            ),
        }
    ).set_index(keys="ColumnA")
    assert result[expected.columns].equals(expected)
    assert all(result["_time_insert"].notna())


def test_insert_add_column(sql):

    table_name = "##test_insert_add_column"
    sql.create.table(table_name, columns={"ColumnA": "TINYINT"})

    dataframe = pd.DataFrame({"ColumnA": [1], "ColumnB": [2], "ColumnC": ["zzz"]})

    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.insert_meta.insert(table_name, dataframe=dataframe)
        assert len(warn) == 3
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column ColumnB in table {table_name} with data type tinyint."
        )
        assert (
            str(warn[2].message)
            == f"Creating column ColumnC in table {table_name} with data type varchar(3)."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(f"SELECT * FROM {table_name}", schema, sql.connection)
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_insert"].notna())


def test_insert_alter_column_unchanged(sql):

    table_name = "##test_insert_alter_column_unchanged"
    sql.create.table(
        table_name,
        columns={"ColumnA": "TINYINT", "ColumnB": "VARCHAR(1)", "ColumnC": "TINYINT"},
    )

    dataframe = pd.DataFrame({"ColumnA": [1], "ColumnB": ["a"], "ColumnC": [1]})
    failure = custom_errors.SQLInsufficientColumnSize(
        "manually testing expection for ColumnB, ColumnC", ["ColumnB", "ColumnC"]
    )
    with pytest.raises(custom_errors.SQLRecastColumnUnchanged):
        _exceptions.handle(
            failure,
            table_name,
            dataframe,
            updating_table=False,
            autoadjust_sql_objects=sql.insert.autoadjust_sql_objects,
            modifier=sql.insert._modify,
            creator=sql.insert._create,
        )


def test_insert_alter_column(sql):

    table_name = "##test_insert_alter_column"
    sql.create.table(
        table_name,
        columns={"ColumnA": "TINYINT", "ColumnB": "VARCHAR(1)", "ColumnC": "TINYINT"},
    )

    dataframe = pd.DataFrame({"ColumnA": [1], "ColumnB": ["aaa"], "ColumnC": [100000]})

    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.insert_meta.insert(table_name, dataframe=dataframe)
        assert len(warn) == 3
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Altering column ColumnB in table {table_name} to data type varchar(3) with is_nullable=True."
        )
        assert (
            str(warn[2].message)
            == f"Altering column ColumnC in table {table_name} to data type int with is_nullable=True."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(f"SELECT * FROM {table_name}", schema, sql.connection)
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_insert"].notna())

    _, dtypes = conversion.sql_spec(schema, dataframe)
    assert dtypes == {
        "ColumnA": "tinyint",
        "ColumnB": "varchar(3)",
        "ColumnC": "int",
        "_time_insert": "datetime2",
    }


def test_insert_alter_primary_key(sql):

    # inital insert
    table_name = "##test_insert_alter_primary_key"
    dataframe = pd.DataFrame(
        {
            "ColumnA": [0, 1, 2, 3],
            "ColumnB": [0, 1, 2, 3],
            "ColumnC": ["a", "b", "c", "d"],
        }
    ).set_index(keys=["ColumnA", "ColumnB"])
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(table_name, dataframe, primary_key="index")
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    schema,_ = conversion.get_schema(sql.connection, table_name)
    _, dtypes = conversion.sql_spec(schema, dataframe)
    assert dtypes == {
        "ColumnA": "tinyint",
        "ColumnB": "tinyint",
        "ColumnC": "varchar(1)",
    }
    assert schema.at["ColumnA", "pk_seq"] == 1
    assert schema.at["ColumnB", "pk_seq"] == 2
    assert pd.isna(schema.at["ColumnC", "pk_seq"])

    # insert that alters primary key
    new = pd.DataFrame(
        {
            "ColumnA": [256, 257, 258, 259],
            "ColumnB": [4, 5, 6, 7],
            "ColumnC": ["e", "f", "g", "h"],
        }
    ).set_index(keys=["ColumnA", "ColumnB"])
    with warnings.catch_warnings(record=True) as warn:
        new = sql.insert.insert(table_name, new)
        assert len(warn) == 1
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == "Altering column ColumnA in table ##test_insert_alter_primary_key to data type smallint with is_nullable=False."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(f"SELECT * FROM {table_name}", schema, sql.connection)
    assert result.equals(dataframe.append(new))
    _, dtypes = conversion.sql_spec(schema, new)
    assert dtypes == {
        "ColumnA": "smallint",
        "ColumnB": "tinyint",
        "ColumnC": "varchar(1)",
    }
    assert schema.at["ColumnA", "pk_seq"] == 1
    assert schema.at["ColumnB", "pk_seq"] == 2
    assert pd.isna(schema.at["ColumnC", "pk_seq"])


def test_insert_add_and_alter_column(sql):

    table_name = "##test_insert_add_and_alter_column"
    dataframe = pd.DataFrame({"ColumnA": [0, 1, 2, 3], "ColumnB": [0, 1, 2, 3]})
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create_meta.table_from_dataframe(table_name, dataframe, primary_key="index")
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    new = pd.DataFrame({
        'ColumnA': [4,5,6,7],
        'ColumnB': [256, 257, 258, 259],
        'ColumnC': [0, 1, 2, 3]
    }, index=[4,5,6,7])
    new.index.name = '_index'

    with warnings.catch_warnings(record=True) as warn:
        new = sql.insert_meta.insert(table_name, new)
        assert len(warn) == 2
        assert all([isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn])
        assert (
            str(warn[0].message)
            == f"Creating column ColumnC in table {table_name} with data type tinyint."
        )
        assert (
            str(warn[1].message)
            == f"Altering column ColumnB in table {table_name} to data type smallint with is_nullable=False."
        )

    schema,_ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(f"SELECT * FROM {table_name}", schema, sql.connection)
    assert result[new.columns].equals(dataframe.append(new))
    assert all(result["_time_insert"].notna())

    _, dtypes = conversion.sql_spec(schema, dataframe)
    assert dtypes == {
        "_index": "tinyint",
        "ColumnA": "tinyint",
        "ColumnB": "smallint",
        "_time_insert": "datetime2",
        "ColumnC": "tinyint",
    }
