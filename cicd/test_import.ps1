# print(f"Creating virtual environment '{build_test_dir}' to test package import.")
# cmd = ["python", "-m", "venv", build_test_dir]
# _ = run_cmd(cmd, venv=False)
# cmd = [f"{build_test_dir}/Scripts/pip", "install", wheel]
# _ = run_cmd(cmd, venv=False)
# cmd = [f"{build_test_dir}/Scripts/python", "-c", f"import {package_name}"]
# print(f"Testing built package import '{' '.join(cmd)}'")
# _ = run_cmd(cmd, venv=False)

$venv = ".\build";
$dist = ".\dist";

if (Test-Path $venv) {
    Write-Output "Removing current virtual environment '$venv'."
    Remove-Item -Path $venv -Recurse
};

Write-Output "Creating virtual environment '$venv'."
python -m venv $venv;

Write-Output "Activating virtual environment '$venv'."
& $venv"\Scripts\Activate.ps1";

$wheel = Get-ChildItem -Path $dist\*.whl -Name
$wheel = "$dist\$wheel"
Write-Output "Installing package '$wheel'."
pip install $wheel;

Write-Output "Testing package import."
python -c "import mssql_dataframe"
Write-Output "Package imported successfully."