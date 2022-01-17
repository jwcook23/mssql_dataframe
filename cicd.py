""" Simulate the CICD process to ensure pull request will succeed. See setup.cfg for associated settings."""
import webbrowser
import os
import subprocess
import configparser

config = configparser.ConfigParser()
config.read("setup.cfg")


def run_cmd(args):
    status = subprocess.run(args, capture_output=True)
    if status.returncode != 0:
        raise RuntimeError(status.stdout)


# black, auto-format to code standard
print(f"running black for module: {config['module']['name']}")
run_cmd(["black", config["module"]["name"]])

# flake8, lint to check code quality
print(f"running flake8 for module: {config['module']['name']}")
run_cmd(
    [
        "flake8",
        config["module"]["name"],
        f"--output-file={config['flake8']['output-file']}",
    ]
)
print(f"generated flake8 statistics file: {config['flake8']['output-file']}")

# black and flake8 for supporting files
for cmd in ["black", "flake8"]:
    for fp in ["tests/", "cicd.py"]:
        print(f"running {cmd} for {fp}")
        run_cmd([cmd, fp])

# pytest & coverage, produce test xml report for genbadge
print(f"running coverage for module: {config['module']['name']}")
print(f"running tests for directory: {config['tool:pytest']['testpaths']}")
run_cmd(
    [
        "coverage",
        "run",
        "-m",
        f"--source={config['module']['name']}",
        "pytest",
        f"--junitxml={config['tool:pytest']['junitxml']}",
    ]
)
print(f"generated coverage sqlite file: {config['coverage:run']['data_file']}")
print(f"generated test xml file: {config['tool:pytest']['junitxml']}")

# coverage xml, for genbadge
run_cmd(["coverage", "xml"])
print(f"generated coverage xml file: {config['coverage:xml']['output']}")

# coverage report, for user viewing
run_cmd(["coverage", "html"])
print(f"generated coverage html file{config['coverage:html']['directory']}")
print("opening coverage report in default webbrowser")
webbrowser.open(
    os.path.join(
        "file:", os.path.abspath(config["coverage:html"]["directory"]), "index.html"
    )
)

# generage badges
badges = {
    "tests": config["tool:pytest"]["junitxml"],
    "coverage": config["coverage:xml"]["output"],
    "flake8": config["flake8"]["output-file"],
}
for b, i in badges.items():
    fp = f"{config['genbadge']['output']}{b}.svg"
    run_cmd(["genbadge", b, "-i", i, "-o", fp])
    print(f"generated badge for {b} at: {fp}")