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
import configparser
import argparse
import glob

from conftest import options


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


def find_markdown_files(config):
    """Find markdown files in current directory. Creates new output directory for markdown tests."""

    markdown_test_directory = f"{config['tool:pytest']['testpaths']}test_markdown/"
    
    if os.path.isdir(markdown_test_directory):
        shutil.rmtree(markdown_test_directory)
    os.mkdir(markdown_test_directory)

    markdown_test_files = {}
    dir = os.getcwd()
    for file_in in os.listdir(dir):
        if file_in.endswith(".md"):
            file_out = file_in.replace(".md", "")
            markdown_test_files[
                file_in
            ] = f"{markdown_test_directory}test_{file_out}.py"

    return markdown_test_files, markdown_test_directory


def check_black_formatting(markdown_test_directory):

    cmd = ["black", ".", "--check", f"--extend-exclude={markdown_test_directory}"]
    print(f"Running '{' '.join(cmd)}' to check code formatting.")
    try:
        _ = run_cmd(cmd)
    except RuntimeError as err:
        raise RuntimeError(
            "black format check failed. Run 'black .' to automatically apply format changes.",
            err.args[0],
        )
    print("black check succeeded.")


def check_flake8_style(config, markdown_test_directory):

    cmd = [
        "flake8",
        "--exclude=env",
        f"--output-file={config['flake8']['output-file']}",
        "--tee",
        f"--extend-exclude={markdown_test_directory}",
    ]
    print(f"Running '{' '.join(cmd)}' to check code style.")
    _ = run_cmd(cmd)
    print(
        f"flake8 check succeeded. Generated flake8 statistics file '{config['flake8']['output-file']}'."
    )


def check_bandit_security(config):

    cmd = ["bandit", "-r", config["options"]["packages"]]
    print(f"Running '{' '.join(cmd)}' to check security.")
    _ = run_cmd(cmd)
    print("bandit check succeeded.")


def check_docstring_formatting(config):

    cmd = ["pydocstyle", config["options"]["packages"], "--convention=numpy"]
    print(f"Running '{' '.join(cmd)}' to check docstring format.")
    _ = run_cmd(cmd)
    print("pydocstyle check succeeded.")


def run_docstring_pytest(config):

    cmd = ["pytest", config["metadata"]["name"], "--doctest-modules"]
    print(f"Running docstring tests using '{' '.join(cmd)}'.")
    _ = run_cmd(cmd)
    print("docstring tests succeeded.")


def generate_markdown_pytest(markdown_test_files):

    for file_in, file_out in markdown_test_files.items():
        cmd = [
            "phmdoctest",
            file_in,
            "--outfile",
            file_out,
        ]
        print(f"Generating markdown test '{' '.join(cmd)}'")
        _ = run_cmd(cmd)


def run_coverage_pytest(config, args):

    # required arguments
    cmd = [
        "coverage",
        "run",
        "--branch",
        "-m",
        f"--source={config['metadata']['name']}",
        "pytest",
        f"--junitxml={config['user:pytest']['junitxml']}",
        "-v",
    ]
    # add optional arguments defined by conftest.py options
    cmd += ["--" + k + "=" + v for k, v in args.items()]

    print(
        f"Running coverage for module '{config['metadata']['name']}' and tests in directory '{config['tool:pytest']['testpaths']}'."
    )

    # use coverage to call pytest
    _ = run_cmd(cmd)
    print(f"Generated coverage sqlite file '{config['coverage:run']['data_file']}'.")
    print(f"Generated test xml file '{config['user:pytest']['junitxml']}'.")


def report_coverage_html(config):

    _ = run_cmd(["coverage", "html"])

    print(
        f"Generated coverage html file '{os.path.join(config['coverage:html']['directory'], 'index.html')}'."
    )


def report_coverage_xml(config):

    _ = run_cmd(["coverage", "xml"])
    print(f"Generated coverage xml file '{config['coverage:xml']['output']}'.")


def generage_package_badges(config):

    badges = {
        "tests": config["user:pytest"]["junitxml"],
        "coverage": config["coverage:xml"]["output"],
        "flake8": config["flake8"]["output-file"],
    }
    for b, i in badges.items():
        fp = f"{config['genbadge']['output']}{b}.svg"
        _ = run_cmd(["genbadge", b, "-i", i, "-o", fp])
        print(f"generating badge for '{b}' at '{fp}'.")


def check_package_version():

    with open("VERSION", "r") as fh:
        version = fh.read()
    print(f"Package version in file 'VERSION' set at '{version}'.")


def build_package():

    outdir = os.path.join(os.getcwd(), "dist")
    print(f"Building package in directory '{outdir}'.")

    # build package .gz and .whl files
    _ = run_cmd(["python", "-m", "build", f"--outdir={outdir}"])
    print(
        f"Built source archives present in {outdir} '{glob.glob(os.path.join(outdir,'*.tar.gz'))}'."
    )
    print(
        f"Built distributions present in {outdir}' {glob.glob(os.path.join(outdir,'*.whl'))}'."
    )

    # check build result
    _ = run_cmd(["twine", "check", os.path.join(outdir, "*")])


# parameters from setup.cfg
config = configparser.ConfigParser()
config.read("setup.cfg")

# command line arguments from confest options since both pytest and argparse use the same parameters
parser = argparse.ArgumentParser()
for opt in options:
    parser.add_argument(opt, **options[opt])
args = parser.parse_args()

# convert args to dictionary to allow to be used as command line args
args = vars(args)
# ignore None as would be passed as "None"
args = {k: v for k, v in args.items() if v is not None}

markdown_test_files, markdown_test_directory = find_markdown_files(config)
check_black_formatting(markdown_test_directory)
check_flake8_style(config, markdown_test_directory)
check_bandit_security(config)
check_docstring_formatting(config)
run_docstring_pytest(config)
generate_markdown_pytest(markdown_test_files)
run_coverage_pytest(config, args)
report_coverage_html(config)
report_coverage_xml(config)
generage_package_badges(config)
check_package_version()
build_package()
