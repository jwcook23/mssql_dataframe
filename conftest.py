r"""Allows for command line arguments for connect.py during testing.

Examples
--------

pytest --server=localhost
"""

import env

import pytest
import pandas

from mssql_dataframe import SQLServer

# define options for both pytest conftest.py and argparse
options = {
    "--database": {"action": "store", "default": "tempdb", "help": "Database name."},
    "--server": {
        "action": "store",
        "default": r"(localdb)\mssqllocaldb",
        "choices": [
            # local SQL Server
            "localhost",
            # local SQL Express
            r"localhost\sqlexpress",
            # Azure DevOps, included with pipeline
            r"(localdb)\mssqllocaldb",
        ],
        "help": "Server name for testing locally or in CICD.",
    },
    "--driver": {
        "action": "store",
        "default": None,
        "help": "ODBC driver name. Default of None will cause the package to infer the best driver to use.",
    }
}


# add options defined above as an option to pytest
def pytest_addoption(parser):
    for opt in options:
        parser.addoption(opt, **options[opt])


# define environment variables for use in tests
def pytest_configure(config):
    env.database = config.getoption("--database")
    env.server = config.getoption("--server")
    env.driver = config.getoption("--driver")


# create namespace functions for testing docstrings
@pytest.fixture(autouse=True)
def add_docstring_namespace(doctest_namespace):
    doctest_namespace["pd"] = pandas

    sql = SQLServer(
        database=env.database, 
        server=env.server, 
        driver=env.driver,
        trusted_connection="yes"
    )
    doctest_namespace["create"] = sql.create
    doctest_namespace["modify"] = sql.modify
    doctest_namespace["read"] = sql.read
    doctest_namespace["insert"] = sql.write.insert
    doctest_namespace["update"] = sql.write.update
    doctest_namespace["merge"] = sql.write.merge
