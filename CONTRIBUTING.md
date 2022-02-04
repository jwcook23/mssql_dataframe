# Contributing Guide

Development and running tests requires a local SQL Server. [SQL Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) can be downloaded for free. The LocalDB package should be selected as LocalDB is readily avialable in Azure DevOps pipelines for the continuous integration and continuous delivery process. To install LocalDB, select Download Media during the installation process, download, and then run SqlLocalDB.msi.

Run all terminal commands in the top level mssql_dataframe folder.

## Development

1. Create a fork/branch of mssql_dataframe/main.

2. Use a python virtual environment for development.

    Create an environment named "env". This environment name is required.

    ``` cmd
    python -m venv env
    ```

    Activate the virtual environment.

    ``` cmd
    .\env\Scripts\activate
    ```

    Install mssql_dataframe in editable mode.

    ``` cmd
    pip install -e .
    ```

    Install additional development requirements.

    ``` cmd
    pip install -r requirements-dev.txt
    ```

3. Perform test driven development.

    Add or adjust tests in the tests folder and use these as the source of development.

4. Run tests.

    ``` cmd
    pytest
    ```

    Troubleshoot test collection if needed.

    ``` cmd
    pytest --collect-only
    ```

    Additional server parameters as defined in `conftest.py options` can be supplied.

    ``` cmd
    pytest --server='localhost'
    ```

5. Run `cicd_template.py`. If this finished to completion the CICD process will finish. Correct any errors as needed.

    ``` cmd
    python cicd_template.py
    ```

    Additional server parameters can also be supplied here.

    ``` cmd
    python cicd_template.py --server='localhost'
    ```

6. Create a Pull Request

## CICD Build Pipelines

Once a pull request is made, the Continuous Integration / Continuous Delievery process begins. An approved pull request review will set the build version, run the CICD pipeline, and uploaded a new package to PyPi.

[CICD Build Pipeline](https://dev.azure.com/jasoncook1989/mssql_dataframe/_build?definitionId=2)

[PyPI mssql-dataframe](https://pypi.org/project/mssql-dataframe/)

### Pull Request Review

1. Reviewer specifies the version number by #TODO version number

    ```txt
    Example Version: 
    A.B.C

    A: major version (backwards incompatiable changes)
    B: minor version (added backwards-compatible functionality)
    C: patch version (bug fixes)
    ```

2. Reviewer triggers the CICD pipeline by adding a comment of `/AzurePipelines run continuous-delivery`
