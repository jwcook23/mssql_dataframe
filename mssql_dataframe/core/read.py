from typing import Literal

import pandas as pd

from mssql_dataframe.core import dynamic, conversion, errors


class read():

    def __init__(self, connection):
        '''Class for reading from SQL tables.
        
        Parameters
        ----------
        connection (mssql_dataframe.connect) : connection for executing statement
        '''

        self.connection = connection


    def select(self, table_name: str, column_names: list = None, where: str = None,
    limit: int = None, order_column: str=None, order_direction: Literal[None,'ASC','DESC'] = None) -> pd.DataFrame:
        """Select data from SQL into a dataframe.

        Parameters
        ----------

        table_name (str) : name of table to select data frame
        column_names (list|str, default=None) : list of columns to select, or None to select all
        where (str, default=None) : where clause filter to apply
        limit (int, default=None) : select limited number of records only
        order_column (str, default=None) : order results by column
        order_direction (str, default=None) : order direction

        Returns
        -------

        dataframe (pandas.DataFrame): tabular data from select statement
        
        None

        Examples
        --------

        #### select entire table

        read.select('SomeTable')

        #### specific columns
        read.select('SomeTable', column_names=['ColumnA','ColumnB']

        #### specify select criteria
        read.select('SomeTable', column_names='ColumnD', where="ColumnB>4 AND ColumnC IS NOT NULL", limit=1, order_column='ColumnB', order_direction='desc')

        """

        # cursor = self.connection.connection.cursor()
        
        # get table schema for conversion to pandas
        schema, _ = conversion.get_schema(self.connection.connection, table_name)

        # always read in primary key columns for dataframe index
        primary_key_columns = list(schema.loc[schema['pk_seq'].notna(),'pk_seq'].sort_values(ascending=True).index)

        # dynamic table and column names, and column_name development
        table_name = dynamic.escape(self.connection.connection.cursor(), table_name)
        if column_names is None:
            column_names = '*'
        else:
            if isinstance(column_names, str):
                column_names = [column_names]
            elif isinstance(column_names, pd.Index):
                column_names = list(column_names)
            column_names = primary_key_columns + column_names
            column_names = list(set(column_names))
            missing = [x for x in column_names if x not in schema.index]
            if len(missing)>0:
                raise errors.SQLColumnDoesNotExist(f'Column does not exist in table {table_name}:', missing)
            column_names = dynamic.escape(self.connection.connection.cursor(), column_names)
            column_names = "\n,".join(column_names)

        # format optional where_statement
        if where is None:
            where_statement, where_args = ("", None)
        else:
            where_statement, where_args = dynamic.where(self.connection.connection.cursor(), where)

        # format optional limit
        if limit is None:
            limit = ""
        elif not isinstance(limit,int):
            raise ValueError("limit must be an integer")
        else:
            limit = "TOP("+str(limit)+")"

        # format optional order
        options = [None,'ASC','DESC']
        if (order_column is None and order_direction is not None) or (order_column is not None and order_direction is None):
            raise ValueError("order_column and order_direction must both be specified")
        elif order_direction not in options:
            raise ValueError("order direction must be one of: "+str(options))
        elif order_column is not None:
            order = "ORDER BY "+dynamic.escape(self.connection.connection.cursor(), order_column)+" "+order_direction
        else:
            order = ""


        # select values
        statement = f"""
        SELECT {limit}
            {column_names}
        FROM
            {table_name}
            {where_statement}
            {order}
        """

        # read sql query
        dataframe = conversion.read_values(statement, schema, self.connection.connection, where_args)

        return dataframe