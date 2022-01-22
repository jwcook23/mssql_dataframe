r""" Run the CICD process to ensure pull request will succeed. Optional arguments are defined in conftest.py.

Examples
--------
cicd.py --server=localhost\SQLEXPRESS

See Also
--------
conftest.py for command line arguments allowed
setup.cfg for associated settings
"""
import os
import subprocess
import configparser
import argparse

from conftest import options


def run_cmd(cmd):
    """Generic command line process and error if needed."""
    status = subprocess.run(cmd, capture_output=True)
    if status.returncode != 0:
        raise RuntimeError(status.stdout)


def run_black(config):
    """black to auto-format code to standard."""
    print(f"running black for module: {config['module']['name']}")
    run_cmd(["black", config["module"]["name"]])


def run_flake8(config):
    """flake8 to lint and check code quality"""
    print(f"running flake8 for module: {config['module']['name']}")
    run_cmd(
        [
            "flake8",
            config["module"]["name"],
            f"--output-file={config['flake8']['output-file']}",
        ]
    )
    print(f"generated flake8 statistics file: {config['flake8']['output-file']}")


def support_file_black_flake8():
    """additionally run black and flake8 for support files."""
    for cmd in ["black", "flake8"]:
        for fp in ["tests/", "cicd.py", "conftest.py"]:
            print(f"running {cmd} for {fp}")
            run_cmd([cmd, fp])


def run_coverage_pytest(config, args):
    """pytest and coverage to ensure code works as desired and is covered by tests. Also produces test xml report for genbadge."""
    print(f"running coverage for module: {config['module']['name']}")
    print(f"running tests for directory: {config['tool:pytest']['testpaths']}")
    # required arguments
    cmd = [
        "coverage",
        "run",
        "--branch",
        "-m",
        f"--source={config['module']['name']}",
        "pytest",
        f"--junitxml={config['user:pytest']['junitxml']}",
        "-v",
    ]
    # add optional arguments defined by conftest.py options
    cmd += ["--" + k + "=" + v for k, v in args.items()]

    # use coverage to call pytest
    run_cmd(cmd)
    print(f"generated coverage sqlite file: {config['coverage:run']['data_file']}")
    print(f"generated test xml file: {config['user:pytest']['junitxml']}")


def coverage_xml(config):
    """coverage xml report for genbadge"""
    run_cmd(["coverage", "xml"])
    print(f"generated coverage xml file: {config['coverage:xml']['output']}")


def coverage_html(config):
    """coverage html report for user viewing"""
    run_cmd(["coverage", "html"])
    print(
        f"generated coverage html file: {os.path.join(config['coverage:html']['directory'], 'index.html')}"
    )


def generage_badges(config):
    """generate badges using genbadge"""
    badges = {
        "tests": config["user:pytest"]["junitxml"],
        "coverage": config["coverage:xml"]["output"],
        "flake8": config["flake8"]["output-file"],
    }
    for b, i in badges.items():
        fp = f"{config['genbadge']['output']}{b}.svg"
        run_cmd(["genbadge", b, "-i", i, "-o", fp])
        print(f"generated badge for {b} at: {fp}")


if __name__ == "__main__":

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

    run_black(config)
    run_flake8(config)
    support_file_black_flake8()
    run_coverage_pytest(config, args)
    coverage_xml(config)
    coverage_html(config)
    generage_badges(config)
    # TODO: build package to PyPi
