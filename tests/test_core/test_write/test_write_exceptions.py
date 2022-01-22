import pytest
import pandas as pd

from mssql_dataframe.core.write import _exceptions


def test_handle():
    with pytest.raises(ValueError):
        _exceptions.handle(
            ValueError("not from mssql_dataframe.core.errors"),
            table_name="failure",
            dataframe=pd.DataFrame({"ColumnA": [100000]}),
            updating_table=False,
            autoadjust_sql_objects=True,
            modifier=None,
            creator=None,
        )
