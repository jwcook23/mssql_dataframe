# Contributing Guide

Testing requires a local SQL Server running with the ability to connect using Windows account credentials.  [SQL Server Developer Edition](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)can be downloaded for free.

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

6. Ensure code is coverged by tests and that tests pass.

    ``` cmd
    python tests.py
    ```

7. Install git hooks using pre-commit to run these tasks automatically.

    - [flake8](https://github.com/psf/black) on commit: lint to check code quality
    - [black](https://github.com/PyCQA/flake8) on commit: auto-format code to standard

    ```cmd
    pre-commit install
    ```

8. Optionally test the pre-commit steps before actually committing.

    ``` cmd
    pre-commit run --all-files
    ```

9. Commit and push the new branch. Create a pull request.

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
