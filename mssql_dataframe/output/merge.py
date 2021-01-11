
class merge():

    def __init__(self):

        self.statement = """
            MERGE {table} AS _target
            USING #{table}_merge AS _source 
            ON {_pk}
            WHEN MATCHED THEN
                UPDATE SET {_update}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({_insert}) VALUES ({_values});
        """

            # WHEN NOT MATCHED BY SOURCE THEN
            #     DELETE  


    def merge(self,dataset):
        """
        Merge dataframe into SQL using a temporary table and a T-SQL MERGE statement.

        Parameters

            dataset         dataframe               data to merge into SQL table
            update          bool, default=True      (WHEN MATCHED)
            insert          bool, default=True      (WHEN NOT MATCHED BY TARGET)
            delete          bool, default=False     (WHEN NOT MATCHED BY SOURCE)

        Returns

            None
        
        """

        statement = self.statement.format(
            _table = table.name,
            _temp = update.name, 
            _pk = ', '.join(['_target.'+x+'=_source.'+x for x in pk]),
            _update = ', '.join(['_target.'+x+'=_source.'+x for x in non_pks]),
            # auto increment
            # _insert = ', '.join(non_pks),
            # _values = ', '.join(['_source.'+x for x in non_pks])
            # non-auto increment
            _insert = ', '.join(pk+non_pks),
            _values = ', '.join(['_source.'+x for x in pk+non_pks])        
        )


    def temp_table(self):
        """
        
        """
        pass