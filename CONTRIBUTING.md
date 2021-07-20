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

3. Install required packages

    ``` cmd
    pip install -r requirements.txt
    ```

4. Install mssql_dataframe in editable mode

    ``` cmd
    pip install -e .
    ```

5. Add additional tests, run existing tests, and view coverage report

    ``` cmd
    pytest --junitxml=reports/test.xml --cov=mssql_dataframe --cov-report=html:reports/coverage --cov-report=xml:reports/coverage.xml
    ```

6. Generate coverage and test badges.

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

2. Install Building & Uploading Packages

    ``` cmd
    pip install twine
    pip install wheel
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
    twine upload --repository-url https://test.pypi.org/legacy/ dist.*
    ```

6. Upload Package to PyPI

    ``` cmd
    twine upload dist/*
    ```

7. Test Installation Locally

    ```cmd
    python -m venv TestDeployment
    ./TestDeployment/Scripts/activate
    pip install mssql_dataframe
    ```
