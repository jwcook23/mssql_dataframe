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