"""Custom Exceptions and Warnings"""


class EnvironmentODBCDriverNotFound(Exception):
    """Exception for not automatically determining ODBC driver."""

    pass


class UndefinedConversionRule(Exception):
    """Exception for undefined conversion rule between pandas, ODBC, and SQL."""


class SQLTableDoesNotExist(Exception):
    """Exception for SQL table that does not exist."""

    pass


class SQLColumnDoesNotExist(Exception):
    """Exception for SQL table column that does not exist."""

    pass


class SQLInvalidSyntax(Exception):
    """Exception for invalid syntax"""

    pass


class SQLUndefinedPrimaryKey(Exception):
    """Exception for undefined SQL primary key in table."""

    pass


class SQLInsufficientColumnSize(Exception):
    """Exception for insufficient column size to insert a numeric or string."""

    pass


class SQLRecastColumnUnchanged(Exception):
    """Exception for SQLInsufficientColumnSize handling not resulting in change of type or size."""

    pass


class SQLRecastColumnChangedCategory(Exception):
    """Exception for SQLInsufficientColumnSize handling resulting in change of data type category."""

    pass


class SQLInvalidLengthObjectName(Exception):
    """Exception for an SQL object name that is too long."""

    pass


class SQLObjectAdjustment(Warning):
    """Warning for creating of modifying an SQL object"""

    pass


class DataframeColumnDoesNotExist(Exception):
    """Exception for undefined dataframe column."""

    pass


class DataframeInvalidDataType(Exception):
    """Exception for a dataframe column that cannot be converted to it's correct type based on the target SQL type."""
