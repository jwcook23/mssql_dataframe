r""" The core of continuous integration / continuous delivery by performing tests,
coverage, formatting, and package building. If errors produced by this script are
corrected, the remove CICD pipeline should complete successfully.

Examples
--------
#### default CI process
python cicd_template.py
#### using command line arguments for server specification
python cicd_template.py --server=localhost\SQLEXPRESS

See Also
--------
CONTRIBUTING.md CICD Build Pipelines for a general overview of the CICD process
conftest.py options variable for server specification parameters
setup.cfg for tests, coverage, and build settings
continuous_integration.yml for Azure DevOps Pipeline CI definition
continuous_deployment.yml for Azure DevOps Pipeline CD definition
"""
import os
import shutil
import subprocess
import argparse
import glob

from conftest import options

package_name = "mssql_dataframe"
venv_dir = "env"
build_dir = "dist"
build_test_dir = "build"
markdown_test_dir = "tests/test_markdown"
pytest_file = "reports/test.xml"
flake8_file = "reports/flake8.txt"
coverage_dir = "reports/coverage"
coverage_file = "reports/.coverage"
coverage_xml = "reports/coverage.xml"
coverage_fail_under = 100
genbadge_dir = "reports"


def run_cmd(cmd, venv=True):
    """Generic command line process and error if needed. Otherwise stdout is returned."""
    # run all commands in virtual environment by default
    if venv:
        cmd[0] = os.path.join(os.getcwd(), "env", "Scripts", cmd[0])
    # call command line process
    status = subprocess.run(cmd, capture_output=True)
    if status.returncode != 0:
        if len(status.stderr) > 0:
            msg = status.stderr.decode("utf-8")
        else:
            msg = status.stdout.decode("utf-8")
        msg = (
            "stderr:\n"
            + status.stderr.decode("utf-8")
            + "\n\nstdout:\n"
            + status.stdout.decode("utf-8")
        )
        raise RuntimeError(msg)

    return status.stdout.decode("utf-8")


def remove_output_dirs():
    for dir in [build_dir, build_test_dir, markdown_test_dir, "reports"]:
        if os.path.exists(dir):
            shutil.rmtree(dir)


def check_black_formatting():

    cmd = ["black", ".", "--check", f"--extend-exclude={markdown_test_dir}"]
    print(f"Checking code format '{' '.join(cmd)}'.")
    try:
        _ = run_cmd(cmd)
    except RuntimeError as err:
        raise RuntimeError(
            f"black format check failed. Run 'black . --extend-exclude={markdown_test_dir}' to automatically apply format changes.",
            err.args[0],
        )


def check_flake8_style():

    exclude = f"{venv_dir}, {markdown_test_dir}, {build_test_dir}"
    cmd = [
        "flake8",
        f"--output-file={flake8_file}",
        "--tee",
        f"--extend-exclude={exclude}",
    ]
    print(f"Checking code style '{' '.join(cmd)}'.")
    _ = run_cmd(cmd)
    print(f"Generated flake8 statistics file '{flake8_file}'.")


def check_bandit_security():

    cmd = ["bandit", "-r", package_name]
    print(f"Checking security '{' '.join(cmd)}'.")
    _ = run_cmd(cmd)


def check_docstring_formatting():

    cmd = ["pydocstyle", package_name, "--convention=numpy"]
    print(f"Checking docstring format '{' '.join(cmd)}'.")
    _ = run_cmd(cmd)


def run_docstring_pytest():

    cmd = ["pytest", package_name, "--doctest-modules"]
    print(f"Running docstring tests '{' '.join(cmd)}'.")
    _ = run_cmd(cmd)


def generate_markdown_pytest():

    os.mkdir(markdown_test_dir)

    markdown_test_files = {}
    dir = os.getcwd()
    for file_in in os.listdir(dir):
        if file_in.endswith(".md"):
            file_out = file_in.replace(".md", "")
            markdown_test_files[file_in] = f"{markdown_test_dir}/test_{file_out}.py"

    for file_in, file_out in markdown_test_files.items():
        cmd = [
            "phmdoctest",
            file_in,
            "--outfile",
            file_out,
        ]
        print(f"Generating markdown test '{' '.join(cmd)}'")
        _ = run_cmd(cmd)


def run_coverage_pytest(args):

    # required arguments
    cmd = [
        "coverage",
        "run",
        "--branch",
        f"--data-file={coverage_file}",
        "-m",
        f"--source={package_name}",
        "pytest",
        f"--junitxml={pytest_file}",
    ]
    # add optional arguments defined by conftest.py options
    cmd += ["--" + k + "=" + v for k, v in args.items()]

    # use coverage to call pytest
    print(f"Running coverage and tests '{' '.join(cmd)}'.")
    _ = run_cmd(cmd)
    print(f"Generated coverage sqlite file '{coverage_file}'.")
    print(f"Generated test xml file '{pytest_file}'.")


def report_coverage_output():

    _ = run_cmd(
        [
            "coverage",
            "html",
            f"--data-file={coverage_file}",
            f"--directory={coverage_dir}",
        ]
    )

    print(f"Generated coverage html file '{os.path.join(coverage_dir, 'index.html')}'.")

    _ = run_cmd(
        [
            "coverage",
            "xml",
            f"--data-file={coverage_file}",
            "-o",
            f"{coverage_xml}",
            f"--fail-under={coverage_fail_under}",
        ]
    )
    print(f"Generated coverage xml file '{coverage_xml}'.")


def generage_package_badges():

    badges = {
        "tests": pytest_file,
        "coverage": coverage_xml,
        "flake8": flake8_file,
    }
    for b, i in badges.items():
        fp = f"{genbadge_dir}/{b}.svg"
        _ = run_cmd(["genbadge", b, "-i", i, "-o", fp])
        print(f"Generated badge for '{b}' at '{fp}'.")


def check_package_version():

    with open("VERSION", "r") as fh:
        version = fh.read()
    print(f"Package version in file 'VERSION' set at '{version}'.")


def build_python_package():

    # build package .gz and .whl files
    cmd = ["python", "-m", "build", f"--outdir={build_dir}"]
    print(f"Building package '{' '.join(cmd)}'.")
    _ = run_cmd(cmd)


def test_python_package():

    # find build files
    source = glob.glob(os.path.join(build_dir, "*.tar.gz"))[0]
    wheel = glob.glob(os.path.join(build_dir, "*.whl"))[0]

    print(f"Built source archive '{source}'.")
    print(f"Built distributions '{wheel}'.")

    # check build result
    cmd = ["twine", "check", os.path.join(build_dir, "*")]
    print(f"Testing built package '{' '.join(cmd)}'")
    _ = run_cmd(cmd)

    # test import of package
    # print(f"Creating virtual environment '{build_test_dir}' to test package import.")
    # cmd = ["python", "-m", "venv", build_test_dir]
    # _ = run_cmd(cmd, venv=False)
    # cmd = [f"{build_test_dir}/Scripts/pip", "install", wheel]
    # _ = run_cmd(cmd, venv=False)
    # cmd = [f"{build_test_dir}/Scripts/python", "-c", f"import {package_name}"]
    # print(f"Testing built package import '{' '.join(cmd)}'")
    # _ = run_cmd(cmd, venv=False)


# command line arguments from confest options since both pytest and argparse use the same parameters
parser = argparse.ArgumentParser()
for opt in options:
    parser.add_argument(opt, **options[opt])
args = parser.parse_args()

# convert args to dictionary to allow to be used as command line args
args = vars(args)
# ignore None as would be passed as "None"
args = {k: v for k, v in args.items() if v is not None}

# remove_output_dirs()
# check_black_formatting()
# check_flake8_style()
# check_bandit_security()
# check_docstring_formatting()
# run_docstring_pytest()
# generate_markdown_pytest()
# run_coverage_pytest(args)
# report_coverage_output()
# generage_package_badges()
# check_package_version()
build_python_package()
test_python_package()
