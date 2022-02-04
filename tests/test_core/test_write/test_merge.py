import env
import warnings

import pytest
import pandas as pd

from mssql_dataframe.connect import connect
from mssql_dataframe.core import custom_warnings, custom_errors, create, conversion
from mssql_dataframe.core.write import merge

pd.options.mode.chained_assignment = "raise"


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.merge = merge.merge(self.connection)
        self.merge_meta = merge.merge(self.connection, include_metadata_timestamps=True)


@pytest.fixture(scope="module")
def sql():
    db = connect(env.database, env.server, env.driver, env.username, env.password)
    yield package(db)
    db.connection.close()


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


def test_merge_upsert(sql):

    table_name = "##test_merge_upsert"
    dataframe = pd.DataFrame({"ColumnA": [3, 4]})
    with warnings.catch_warnings(record=True) as warn:
        sql.create.table_from_dataframe(table_name, dataframe, primary_key="index")
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # delete, but keep in SQL since upserting
    dataframe = dataframe[dataframe.index != 0].copy()
    # update
    dataframe.loc[dataframe.index == 1, "ColumnA"] = 5
    # insert
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame([6], columns=["ColumnA"], index=pd.Index([2], name="_index")),
        ]
    )

    # merge values into table, using the SQL primary key that came from the dataframe's index
    dataframe = sql.merge.merge(table_name, dataframe, upsert=True)

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert dataframe.equals(result.loc[[1, 2]])
    assert result.loc[0].equals(pd.Series([3], dtype="UInt8", index=["ColumnA"]))
    assert "_time_update" not in result.columns
    assert "_time_insert" not in result.columns


def test_merge_one_match_column(sql):

    table_name = "##test_merge_one_match_column"
    dataframe = pd.DataFrame({"ColumnA": [3, 4]})
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # delete
    dataframe = dataframe[dataframe.index != 0]
    # update
    dataframe.loc[dataframe.index == 1, "ColumnA"] = 5
    # insert
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame([6], columns=["ColumnA"], index=pd.Index([2], name="_index")),
        ]
    )

    # merge values into table, using the SQL primary key that came from the dataframe's index
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(table_name, dataframe)
        assert len(warn) == 2
        assert all(
            [isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn]
        )
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna() == [True, False])
    assert all(result["_time_insert"].notna() == [False, True])


def test_merge_two_match_columns(sql):

    table_name = "##test_merge_two_match_columns"
    dataframe = pd.DataFrame(
        {"State": ["A", "B"], "ColumnA": [3, 4], "ColumnB": ["a", "b"]}
    )
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # delete
    dataframe = dataframe[dataframe.index != 0]
    # update
    dataframe.loc[dataframe.index == 1, "ColumnA"] = 5
    # insert
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame(
                {"State": ["C"], "ColumnA": [6], "ColumnB": ["d"]},
                index=pd.Index([2], name="_index"),
            ),
        ]
    )

    # merge values into table, using the primary key that came from the dataframe's index and ColumnA
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(
            table_name, dataframe, match_columns=["_index", "State"]
        )
        assert len(warn) == 2
        assert all(
            [isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn]
        )
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert all(result["_time_update"].notna() == [True, False])
    assert all(result["_time_insert"].notna() == [False, True])


def test_merge_non_pk_column(sql):

    table_name = "##test_merge_non_pk_column"
    dataframe = pd.DataFrame(
        {"State": ["A", "B"], "ColumnA": [3, 4], "ColumnB": ["a", "b"]}
    )
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key=None
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # delete
    dataframe = dataframe[dataframe.index != 0]
    dataframe = dataframe.reset_index(drop=True)
    # update
    dataframe.loc[dataframe.index == 1, "ColumnA"] = 5
    # insert
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame(
                {"State": ["C"], "ColumnA": [6], "ColumnB": ["d"]},
                index=pd.Index([1], name="_index"),
            ),
        ]
    )

    # merge values into table, using a single column that is not the primary key
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(table_name, dataframe, match_columns=["State"])
        assert len(warn) == 2
        assert all(
            [isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn]
        )
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name} ORDER BY _time_update DESC",
        schema,
        sql.connection,
    )
    assert result[dataframe.columns].equals(dataframe)


def test_merge_composite_pk(sql):

    table_name = "##test_merge_composite_pk"
    dataframe = pd.DataFrame(
        {"State": ["A", "B"], "ColumnA": [3, 4], "ColumnB": ["a", "b"]}
    ).set_index(keys=["State", "ColumnA"])
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # delete
    dataframe = dataframe[dataframe.index != ("A", 3)].copy()
    # update
    dataframe.loc[dataframe.index == ("B", 4), "ColumnB"] = "c"
    # insert
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame({"State": ["C"], "ColumnA": [6], "ColumnB": ["d"]}).set_index(
                keys=["State", "ColumnA"]
            ),
        ]
    )
    dataframe = sql.merge.merge(table_name, dataframe)

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert result[dataframe.columns].equals(dataframe)
    assert "_time_update" not in result
    assert "_time_insert" not in result


def test_merge_one_delete_condition(sql):

    table_name = "##test_merge_one_delete_condition"
    dataframe = pd.DataFrame(
        {"State": ["A", "B", "B"], "ColumnA": [3, 4, 4], "ColumnB": ["a", "b", "b"]},
        index=[0, 1, 2],
    )
    dataframe.index.name = "_pk"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # delete 2 records
    dataframe = dataframe[dataframe.index == 1].copy()
    # update 1 record
    dataframe.loc[dataframe.index == 1, ["ColumnA", "ColumnB"]] = [5, "c"]
    # insert 1 record
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame(
                {"State": ["C"], "ColumnA": [6], "ColumnB": ["d"]},
                index=pd.Index([3], name="_index"),
            ),
        ]
    )

    # merge values into table, using the primary key that came from the dataframe's index
    # prevent _pk 0 from being deleted as source dataframe must contain a match for state
    dataframe.index.name = "_pk"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(
            table_name, dataframe, match_columns=["_pk"], delete_requires=["State"]
        )
        assert len(warn) == 2
        assert all(
            [isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn]
        )
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert all(result.loc[[1, 3], ["State", "ColumnA", "ColumnB"]] == dataframe)
    assert all(
        result.loc[0, ["State", "ColumnA", "ColumnB"]]
        == pd.Series(["A", 3, "a"], index=["State", "ColumnA", "ColumnB"])
    )
    assert all(result["_time_update"].notna() == [False, True, False])
    assert all(result["_time_insert"].notna() == [False, False, True])


def test_merge_two_delete_requires(sql):

    table_name = "##test_merge_two_delete_requires"
    dataframe = pd.DataFrame(
        {
            "State1": ["A", "B", "B"],
            "State2": ["X", "Y", "Z"],
            "ColumnA": [3, 4, 4],
            "ColumnB": ["a", "b", "b"],
        },
        index=[0, 1, 2],
    )
    dataframe.index.name = "_pk"
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.create.table_from_dataframe(
            table_name, dataframe, primary_key="index"
        )
        assert len(warn) == 1
        assert isinstance(warn[0].message, custom_warnings.SQLObjectAdjustment)
        assert "Created table" in str(warn[0].message)

    # delete 2 records
    dataframe = dataframe[dataframe.index == 1].copy()
    # update
    dataframe.loc[dataframe.index == 1, ["ColumnA", "ColumnB"]] = [5, "c"]
    # insert
    dataframe.index.name = "_pk"
    dataframe = pd.concat(
        [
            dataframe,
            pd.DataFrame(
                {"State1": ["C"], "State2": ["Z"], "ColumnA": [6], "ColumnB": ["d"]},
                index=pd.Index([3], name="_pk"),
            ),
        ]
    )

    # merge values into table, using the primary key that came from the dataframe's index
    # also require a match on State1 and State2 to prevent a record from being deleted
    with warnings.catch_warnings(record=True) as warn:
        dataframe = sql.merge_meta.merge(
            table_name,
            dataframe,
            match_columns=["_pk"],
            delete_requires=["State1", "State2"],
        )
        assert len(warn) == 2
        assert all(
            [isinstance(x.message, custom_warnings.SQLObjectAdjustment) for x in warn]
        )
        assert (
            str(warn[0].message)
            == f"Creating column _time_update in table {table_name} with data type DATETIME2."
        )
        assert (
            str(warn[1].message)
            == f"Creating column _time_insert in table {table_name} with data type DATETIME2."
        )

    schema, _ = conversion.get_schema(sql.connection, table_name)
    result = conversion.read_values(
        f"SELECT * FROM {table_name}", schema, sql.connection
    )
    assert all(
        result.loc[[1, 3], ["State1", "State2", "ColumnA", "ColumnB"]] == dataframe
    )
    assert all(
        result.loc[0, ["State1", "State2", "ColumnA", "ColumnB"]]
        == pd.Series(
            ["A", "X", 3, "a"], index=["State1", "State2", "ColumnA", "ColumnB"]
        )
    )
    assert all(result["_time_update"].notna() == [False, True, False])
    assert all(result["_time_insert"].notna() == [False, False, True])
