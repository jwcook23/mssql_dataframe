import pathlib
from setuptools import setup, find_packages

README = (pathlib.Path(__file__).parent/"README.md").read_text()

setup(
    name = "mssql_dataframe",
    version = "1.0.6",
    license='MIT',
    license_files="LICENSE",
    description="Update, Upsert, and Merge from Python dataframes to SQL Server and Azure SQL database.",
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/jwcook23/mssql_dataframe',
    author="Jason Cook",
    author_email="jasoncook1989@gmail.com",
    python_requires='>=3.5',
    packages = find_packages(exclude=('tests',)),
    install_requires = [
        'pyodbc',
        'pandas'
    ]
)