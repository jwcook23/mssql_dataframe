'''Custom Exceptions'''

class GeneralError(Exception):
    '''General error to prevent exposing SQL server specific error messages.'''
    pass

class ODBCDriverNotFound(Exception):
    '''Exception for not automatically determining ODBC driver.'''
    pass

class TableDoesNotExist(Exception):
    '''Exception for SQL table that does not exist.'''
    pass

class ColumnDoesNotExist(Exception):
    '''Exception for SQL table column that does not exist.'''
    pass

class InvalidSyntax(Exception):
    '''Exception for invalid syntax'''
    pass

class UndefinedPythonDataType(Exception):
    '''Exception for undefined SQL equalivant Python data type.'''
    pass

class UndefinedPrimaryKey(Exception):
    '''Exception for undefined SQL primary key in table.'''
    pass

class InsufficientColumnSize(Exception):
    '''Exception for insufficient column size to insert a value.'''
    pass