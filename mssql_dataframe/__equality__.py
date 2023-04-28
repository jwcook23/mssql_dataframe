"""Equality checks with failure reasons for dataframes."""


def compare_dfs(df1, df2):
    """Raise descriptive exception if dataframes are not equal."""
    if df1.equals(df2):
        return True

    assert df1.columns.equals(df2.columns)

    assert df1.index.equals(df2.index)

    for col in df1.columns:
        assert df1[col].equals(df2[col]), f"Column {col} is not equal."
