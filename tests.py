import webbrowser
import os
import pytest

exitcode = pytest.main(
    [
        # produce coverage report using pytest-cov plugin
        "--cov=mssql_dataframe",
        # require 100% test coverage
        "--cov-fail-under=100",
        # test detail to XML for genbadge
        "--junitxml=reports/test.xml",
        # coverage detail to XML for genbadge
        "--cov-report=xml:reports/coverage.xml",
        # user friendly coverage to HTML
        "--cov-report=html:reports/coverage",
    ]
)
print(f"pytest exit code: {str(exitcode)}")

print("Opening coverage report in default webbrowser.")
fp = os.path.join("file:", os.getcwd(), "reports", "coverage", "index.html")
webbrowser.open(fp)
