$venv = ".\env";
$required = "requirements-dev.txt"

if (Test-Path $venv) {
    Write-Output "Removing current virtual environment '$venv'."
    Remove-Item -Path $venv -Recurse
};

Write-Output "Creating virtual environment '$venv'."
python -m venv $venv;

Write-Output "Activating virtual environment '$venv'."
& "$venv\Scripts\Activate.ps1";

Write-Output "Updating pip to latest version."
python -m pip install --upgrade pip;

Write-Output "Installing development requirements from '$required'."
pip install -r $required --upgrade;

Write-Output "Installing package in editable mode."
pip install -e .;