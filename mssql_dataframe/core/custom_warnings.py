"""Custom warnings for mssql_dataframe."""


class SQLObjectAdjustment(Warning):
    """Warning for creating of modifying an SQL object"""

    pass


class SQLDataTypeTIMETruncation(Warning):
    """SQL TIME only supports 7 decimal places for precision but pandas Timedelta supports 9."""

    pass


class SQLDataTypeDATETIME2Truncation(Warning):
    """SQL DATETIME2 only supports 7 decimal places for precision but pandas Timestamp supports 9."""

    pass