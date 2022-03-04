"""Methods for creating, modifying, reading, and writing between dataframes and SQL."""
import logging
from importlib.metadata import version
from mssql_dataframe.package import SQLServer  # noqa: F401

# set version number
__version__ = version("mssql_dataframe")

# initialize logging
logging.getLogger(__name__)
