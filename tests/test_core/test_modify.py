import env

import pytest

from mssql_dataframe.connect import connect
from mssql_dataframe.core import create, modify, conversion


class package:
    def __init__(self, connection):
        self.connection = connection.connection
        self.create = create.create(self.connection)
        self.modify = modify.modify(self.connection)


@pytest.fixture(scope="module")
def sql():
    db = connect(database=env.database, server=env.server, trusted_connection="yes")
    yield package(db)
    db.connection.close()


def test_column_input_error(sql):
    table_name = "##column_input_error"
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    sql.create.table(table_name, columns)

    with pytest.raises(ValueError) as error:
        sql.modify.column(table_name, modify="delete", column_name="B")
    assert "modify must be one of: " in str(error)


def test_column_drop(sql):
    table_name = "##column_drop"
    columns = {"A": "VARCHAR", "B": "VARCHAR"}
    sql.create.table(table_name, columns)

    sql.modify.column(table_name, modify="drop", column_name="B")

    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert "B" not in schema.index


def test_column_add(sql):
    table_name = "##test_column_add"
    columns = {"A": "VARCHAR"}
    sql.create.table(table_name, columns)

    sql.modify.column(
        table_name, modify="add", column_name="B", data_type="VARCHAR(20)"
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert "B" in schema.index
    assert schema.at["B", "sql_type"] == "varchar"

    sql.modify.column(table_name, modify="add", column_name="C", data_type="BIGINT")
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert "C" in schema.index
    assert schema.at["C", "sql_type"] == "bigint"


def test_column_alter(sql):
    table_name = "##test_column_alter"
    columns = {"A": "VARCHAR(10)", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns)

    sql.modify.column(table_name, modify="alter", column_name="B", data_type="INT")
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert "B" in schema.index
    assert schema.at["B", "sql_type"] == "int"
    assert schema.at["B", "is_nullable"]

    sql.modify.column(
        table_name, modify="alter", column_name="C", data_type="INT", is_nullable=False
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert "C" in schema.index
    assert schema.at["C", "sql_type"] == "int"
    assert not schema.at["C", "is_nullable"]


def test_primary_key_input_error(sql):
    table_name = "##test_primary_key_input_error"
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns, not_nullable=["A", "B"])

    with pytest.raises(ValueError):
        sql.modify.primary_key(
            table_name, modify="create", columns=["A", "B"], primary_key_name="_pk_1"
        )


def test_primary_key_one_column(sql):
    table_name = "##test_primary_key_one_column"
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns, not_nullable=["A", "B"])

    sql.modify.primary_key(
        table_name, modify="add", columns="A", primary_key_name="_pk_1"
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert schema.at["A", "pk_seq"] == 1
    assert sum(schema["pk_seq"].notna()) == 1

    sql.modify.primary_key(
        table_name, modify="drop", columns="A", primary_key_name="_pk_1"
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert all(schema["pk_seq"].isna())


def test_primary_key_two_columns(sql):
    table_name = "##test_primary_key_two_columns"
    columns = {"A": "INT", "B": "BIGINT", "C": "BIGINT", "D": "BIGINT"}
    sql.create.table(table_name, columns, not_nullable=["A", "B"])

    sql.modify.primary_key(
        table_name, modify="add", columns=["A", "B"], primary_key_name="_pk_1"
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert schema.at["A", "pk_seq"] == 1
    assert schema.at["B", "pk_seq"] == 2
    assert sum(schema["pk_seq"].notna()) == 2

    sql.modify.primary_key(
        table_name, modify="drop", columns=["A", "B"], primary_key_name="_pk_1"
    )
    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert all(schema["pk_seq"].isna())


def test_alter_primary_key_column(sql):
    table_name = "##test_alter_primary_key_column"
    columns = {"_pk": "TINYINT", "A": "VARCHAR(1)"}
    primary_key_column = "_pk"
    sql.create.table(table_name, columns, primary_key_column="_pk")

    schema, _ = conversion.get_schema(sql.connection, table_name)
    primary_key_name = schema.at[primary_key_column, "pk_name"]

    sql.modify.primary_key(
        table_name,
        modify="drop",
        columns=primary_key_column,
        primary_key_name=primary_key_name,
    )
    sql.modify.column(
        table_name,
        modify="alter",
        column_name=primary_key_column,
        data_type="INT",
        is_nullable=False,
    )
    sql.modify.primary_key(
        table_name,
        modify="add",
        columns=primary_key_column,
        primary_key_name=primary_key_name,
    )

    schema, _ = conversion.get_schema(sql.connection, table_name)
    assert schema.at[primary_key_column, "sql_type"] == "int"
    assert schema.at[primary_key_column, "pk_seq"] == 1
