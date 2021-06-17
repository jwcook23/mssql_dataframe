from setuptools import setup, find_packages
setup(
    name = "mssql_dataframe",
    version = "0.1.0",
    author="Jason Cook",
    author_email="jasoncook1989@gmail.com",
    license='MIT',
    license_files="LICENSE",
    description="Easy interactions between MSSQL and Python DataFrames.",
    python_requires='>=3.8',
    packages = find_packages(),
    install_requires = [
        'pyodbc',
        'pandas'
    ]
)