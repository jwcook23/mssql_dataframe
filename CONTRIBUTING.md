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

## CICD Build Pipelines

Continuous Integration Continous Delievery can be accomplished in multiple ways.

1. Remote Azure DevOps pipeline. This is the default build process that happens with a pull request.
2. Local Azure DevOps pipeline for testing.
3. Local `cicd.py` for CICD process testing build without Azure.

### 1. Remote Azure DevOps

<!-- #TODO: document default build process -->
[Azure Pipeline Agents](https://docs.microsoft.com/en-us/azure/devops/pipelines/agents/agents?view=azure-devops&tabs=browser#install)

### 2. Local Azure DevOps

[Local Windows Agent](https://docs.microsoft.com/en-us/azure/devops/pipelines/agents/v2-windows?view=azure-devops)

If you have previously configured the agent, simply start it. The agent should then show online in Azure DevOps > Organization settings >  Agent pools > Agent.

``` ps
cd azure-local-pipeline;
.\run.cmd;
```

Otherwise complete the following steps, starting with opening [Azure DevOps](https://dev.azure.com/jasoncook1989/) to begin setting up the local machine.

#### Add New Agent Pool

1. Orgnaization settings > Agent pools > Add pool
2. Pool type = self-hosted
3. Name = Local Laptop

#### Download Agent

1. Select the newly created pool.
2. Download the agent.

#### Create Agent Locally in PowerShell

First change the directory to the mssql_dataframe repository so the agent directory is created there.

``` ps
cd C:\Users\jacoo\Desktop\Code\python-lib\mssql_dataframe
```

Second extract the downloaded zip file containing the agent. Note the System.IO.Compression.ZipFile command depends on the downloaded version.

``` ps
mkdir azure-local-pipeline;
cd azure-local-pipeline;
Add-Type -AssemblyName System.IO.Compression.FileSystem;
[System.IO.Compression.ZipFile]::ExtractToDirectory("$HOME\Downloads\vsts-agent-win-x64-2.196.1.zip", "$PWD")
```

#### Configure the Agent in PowerShell

``` ps
.\config.cmd
Server URL = https://dev.azure.com/jasoncook1989
```

Press enter to select the personal access token (PAT) method.

#### Create PAT for PowerShell

1. Azure DevOps homepage > user settings (intials icon) > Security > Personal access tokens > New Token
2. Name = azure-local-pipeline
3. Scope = Agent Pools > Read & manage

#### Register Agent

``` ps
agent pool = Local Laptop
agent name > use default by pressing enter
work folder > use default by pressing enter
run agent as service > N
```

#### Run Agent

``` ps
cd azure-local-pipeline;
.\run.cmd;
```

### 3. Local cicd.py

Simply run the script.

``` cmd
python cicd.py
```

## Python Package Index (PyPI)

1. Increment the version in setup.py as appropriate. Given example version A.B.C

    <!--#TODO: implement script to build and increment version-->

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
