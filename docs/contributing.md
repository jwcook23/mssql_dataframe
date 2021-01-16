# Directory
All follwing operations should take place at the mssql_dataframe root directory. 

C:/PATH/TO/mssql_dataframe
```
mssql_dataframe         < root directory
|-mssql_dataframe
|-tests
|-requirements.txt
|-README.me
|-setup.py
|-LICENSE
```

# Virutal Environment

Create Virutal Environment
```
python3 -m venv env
```

Activate Depending on Your Operating Systems
```
# Windows
.\env\Scripts\activate

# Unix or MacOS
source env/bin/activate
```

# Generate Distribution Archives

Ensure latest versions of setuptools and wheel is installed.
```
python3 -m pip install --user --upgrade setuptools wheel
```

Generage archive folders.
```
python3 setup.py sdist bdist_wheel
```

# Install in Editable Mode

Install from the current working directory. Don't forget the "dot"!

```
pip install -e .
```