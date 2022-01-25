import pathlib
from setuptools import setup, find_packages

README = (pathlib.Path(__file__).parent / "README.md").read_text()

setup(
    name="mssql_dataframe",
    # version=0 to specify on command line like: python setup.py sdist bdist_wheel egg_info --tag-build=1.1.3
    # version=0,
    license="MIT",
    license_files="LICENSE",
    description="Update, Upsert, and Merge from Python dataframes to SQL Server and Azure SQL database.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/jwcook23/mssql_dataframe",
    author="Jason Cook",
    author_email="jasoncook1989@gmail.com",
    python_requires=">=3.5",
    packages=find_packages(exclude=("tests",)),
    # range of versions required with reason for minimum version and max version at level using to develop
    install_requires=[
        # Cursor.setinputsizes to specify odbc data type and size
        "pyodbc>=4.0.24, <=4.0.32",
        # expanded data types such as pandas.UInt8Dtype and pd.StringDtype
        "pandas>=1.0.0, <=1.3.5",
    ],
)
