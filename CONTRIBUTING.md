# Contributing Guide

Run all terminal commands in the top level mssql_dataframe folder. SQL Server Developer Edition can be downloaded for free [here](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

1. Create virtual environment.
``` cmd
python -m venv env
```

2. Activate virtual environment.
``` cmd
.\env\Scripts\activate
```

3. Install required packages.
``` cmd
pip install -r requirements.txt
```

4. Install mssql_dataframe in editable mode.
``` cmd
pip install -e .
```

5. Run tests and view coverage report.
``` cmd
pytest --cov-report html --cov=mssql_dataframe
```
