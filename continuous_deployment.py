r""" Continuous deployment by rebuilding the package and uploading to PyPi. Executed after continuous_integration.py.

Examples
--------
continuous_integration.py --server=localhost\SQLEXPRESS

See Also
--------
conftest.py for command line arguments allowed
setup.cfg for associated settings
"""
from continuous_integration import run_cmd
import os
import shutil

def build_package():
    "build Python package for upload to PyPi.org"

    dist = "./dist"
    print(f"building package in directory: {dist}")

    # create or empty ./dist folder that may exist from previous builds
    if os.path.exists(dist):
        shutil.rmtree(dist)
    os.makedirs(dist)

    # build package .gz and .whl files
    run_cmd(["python", "setup.py", "sdist", "bdist_wheel"])

    # check build status
    run_cmd(["twine", "check", "dist/*"])


if __name__ == "__main__":

    build_package()
