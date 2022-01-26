r"""Allows for command line arguments for connect.py during testing.

Examples
--------

pytest --server=localhost
"""

import env

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
    },
    "--username": {
        "action": "store",
        "default": None,
        "help": "Username to use to connect to server.",
    },
    "--password": {
        "action": "store",
        "default": None,
        "help": "Password to use to connect to server.",
    },
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
    env.username = config.getoption("--username")
    env.password = config.getoption("--password")
