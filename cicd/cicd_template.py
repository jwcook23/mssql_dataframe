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
        raise RuntimeError(msg)

    return status.stdout.decode("utf-8")


def check_black():
    """Check if black formatting passes. If not, suggest running."""
    print("running black for all Python files not excluded by .gitignore")

    try:
        _ = run_cmd(["black", ".", "--check"])
    except RuntimeError as err:
        raise RuntimeError(
            "black format check did not pass. Try running 'black . --diff' to see what needs formatted then 'black .' to automatically format.",
            err.args[0],
        )


def check_flake8(config):
    """Run flake8 to lint and check code quality."""
    print(
        "running flake8 for all Python files excluding virtual environment directory named 'env'"
    )
    _ = run_cmd(
        [
            "flake8",
            "--exclude=env",
            f"--output-file={config['flake8']['output-file']}",
            "--tee",
        ]
    )
    print(f"generated flake8 statistics file: {config['flake8']['output-file']}")


def check_precommit():
    """Check if pre-commit hooks pass."""
    print("installing pre-commit hooks")
    _ = run_cmd(["pre-commit", "install"])
    print("checking if pre-commit hooks pass")
    _ = run_cmd(["pre-commit", "run", "--all-files"])


def run_coverage_pytest(config, args):
    """Run pytest and coverage to ensure code works as desired and is covered by tests. Also produces test xml report for genbadge."""
    print(f"running coverage for module: {config['metadata']['name']}")
    print(f"running tests for directory: {config['tool:pytest']['testpaths']}")
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

    # use coverage to call pytest
    _ = run_cmd(cmd)
    print(f"generated coverage sqlite file: {config['coverage:run']['data_file']}")
    print(f"generated test xml file: {config['user:pytest']['junitxml']}")


def coverage_html(config):
    """Generage coverage html report for user viewing."""
    print(
        f"generating coverage html file: {os.path.join(config['coverage:html']['directory'], 'index.html')}"
    )
    _ = run_cmd(["coverage", "html"])


def coverage_xml(config):
    """Generate coverage xml report for genbadge."""
    print(f"generating coverage xml file: {config['coverage:xml']['output']}")
    _ = run_cmd(["coverage", "xml"])


def generage_badges(config):
    """Generate badges using genbadge."""
    badges = {
        "tests": config["user:pytest"]["junitxml"],
        "coverage": config["coverage:xml"]["output"],
        "flake8": config["flake8"]["output-file"],
    }
    for b, i in badges.items():
        fp = f"{config['genbadge']['output']}{b}.svg"
        print(f"generating badge for {b} at: {fp}")
        _ = run_cmd(["genbadge", b, "-i", i, "-o", fp])


def check_version():
    "Check the package number."

    with open("VERSION", "r") as fh:
        version = fh.read()
    print(f"Package version set by PowerShell script cicd_version.ps1: {version}")


def build_package():
    "Build Python package."

    outdir = os.path.join(os.getcwd(), "dist")
    print(f"building package in directory: {outdir}")

    # build package .gz and .whl files
    _ = run_cmd(["python", "-m", "build", f"--outdir={outdir}"])
    print(
        f"built source archives present in {outdir}: {glob.glob(os.path.join(outdir,'*.tar.gz'))}"
    )
    print(
        f"built distributions present in {outdir}: {glob.glob(os.path.join(outdir,'*.whl'))}"
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

check_black()
check_flake8(config)
check_precommit()
run_coverage_pytest(config, args)
coverage_html(config)
coverage_xml(config)
generage_badges(config)
check_version()
build_package()