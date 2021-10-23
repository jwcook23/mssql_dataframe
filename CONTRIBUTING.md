# Contributing Guide

Testing requires a local SQL Server running with the ability to connect using Windows account credentials.  [SQL Server Developer Edition](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)can be downloaded for free.

Run all terminal commands in the top level mssql_dataframe folder.

## Run Tests and Code Coverage Report

1. Create virtual environment

    ``` cmd
    python -m venv env
    ```

2. Activate virtual environment

    ``` cmd
    .\env\Scripts\activate
    ```

3. Install/update required testing, formatting, and coverage packages

    ``` cmd
    pip install --upgrade pytest pytest-cov pytest-flake8 genbadge[tests,coverage] black
    ```

4. Install mssql_dataframe in editable mode and make changes

    ``` cmd
    pip install -e .
    ```

5. Format code to pep8 standards using flake8 & black

    ``` cmd
    black mssql_dataframe
    flake8 mssql_dataframe
    ```

6. Add additional tests, run existing tests, and view coverage report

    ``` cmd
    pytest --junitxml=reports/test.xml --cov=mssql_dataframe --cov-report=html:reports/coverage --cov-report=xml:reports/coverage.xml
    ```

7. Generate coverage and test badges.

    ```cmd
    genbadge tests -i reports/test.xml -o reports/tests.svg
    genbadge coverage -i reports/coverage.xml -o reports/coverage.svg
    ```

## Python Package Index (PyPI)

1. Increment the version in setup.py as appropriate. Given example version A.B.C

    ```txt
    A.B.C

    A: major version (backwards incompatiable changes)
    B: minor version (added backwards-compatible functionality)
    C: patch version (bug fixes)
    ```

2. Install/update Building & Uploading Packages

    ``` cmd
    pip install --upgrade twine setuptools wheel
    ```

3. Remove Old Build Files

    Ensure the ./dist folder does not contain .gz and .whl files for previous build versions.

4. Build Package & Test

    ``` cmd
    python setup.py sdist bdist_wheel
    twine check dist/*
    ```

5. Upload Package to Test PyPI

    ``` cmd
    twine upload --repository-url https://test.pypi.org/legacy/ dist/*
    ```

6. Test Installation Locally

    ```cmd
    python -m venv TestDeployment
    ./TestDeployment/Scripts/activate
    pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ mssql_dataframe
    ```

7. Upload Package to PyPI

    ``` cmd
    twine upload dist/*
    ```
