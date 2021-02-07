from datetime import datetime, date

import pandas as pd


class sample():
    
    def __init__(self):
        
        self.dataframe = pd.DataFrame({
            '_varchar': [None,'b','c','d','e'],
            '_tinyint': [None,2,3,4,5],
            '_int': [1,2,3,4,5],
            '_bigint': [1,2,3,4,9999999999],
            '_numeric': [1.11,2,3,4,None],
            '_float': [1.111111,2,3,4,5],
            '_date': [date.today()]*5,
            '_time': [datetime.now().time()]*5,
            '_datetime': [datetime.now()]*4+[pd.NaT]  
        })
        self.dataframe['_tinyint'] = self.dataframe['_tinyint'].astype('Int64')

        self.columns  = {
            '_varchar': 'VARCHAR(255)',
            '_tinyint': 'TINYINT',
            '_int': 'INT',
            '_bigint': 'BIGINT',
            '_numeric': 'NUMERIC(20,4)',
            '_float': 'FLOAT',
            '_date': 'DATE',
            '_time': 'TIME',
            '_datetime': 'DATETIME'         
        }

        self.pk = '_int'

        self.notnull = ['_bigint','_float']