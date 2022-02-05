# Write the package version to the file named VERSION.
# Occurs automatically in the CICD pipeline but must be performed manually for local testing.
# Example: .\cicd_version.ps1 0.0.0

param(
    [string]$tag
)

Write-Output "Input version parameter:  $($tag)"

if ($tag -match '(?<version>\d+\.\d+\.\d+).*')
{
    Write-Output "Writing version to file named VERSION:  $($Matches.version)"
    $Matches.version | Out-File -FilePath VERSION -Encoding ASCII -NoNewline;
}
else 
{
    Write-Error -Message "Unable to parse input version. Expected param is in the form 0.0.0" -ErrorAction Stop
}