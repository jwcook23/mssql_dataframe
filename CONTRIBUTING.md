# Contributing Guide

Testing requires a local SQL Server running with the ability to connect using Windows account credentials.  [SQL Server Developer Edition or SQL Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) can be downloaded for free.

Run all terminal commands in the top level mssql_dataframe folder. The below commands are specific to a Windows systems.

## Run Tests and Code Coverage Report

1. Use Git to clone the main branch and create a new branch.

2. Create python virtual environment.

    ``` cmd
    python -m venv env
    ```

3. Activate virtual environment.

    ``` cmd
    .\env\Scripts\activate
    ```

4. Install development depenancies.

    ``` cmd
    pip install -r requirements-dev.txt
    ```

5. Install mssql_dataframe in editable mode and make changes to code and tests.

    ``` cmd
    pip install -e .
    ```

6. Ensure code will pass later CICD processes. This will run coverage, tests, and other build tasks.

    ``` cmd
    python cicd.py
    ```

    Steps such as pytest can be ran manually during development. Pytest will accept the same arguments as `mssql_dataframe.connect.py`.

    ``` cmd
    pytest --server='localhost'
    ```

7. Install git hooks using pre-commit to check files before committing.

    ```cmd
    pre-commit install
    ```

    Optionally test the pre-commit steps before actually preforming the commit.

    ``` cmd
    pre-commit run --all-files
    ```

8. Commit and push the new branch. Create a pull request.

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
