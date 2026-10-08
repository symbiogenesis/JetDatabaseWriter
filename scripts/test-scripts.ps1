# Parse every PowerShell script and run isolated, self-contained script regression checks.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
foreach ($file in Get-ChildItem -LiteralPath $PSScriptRoot -Filter '*.ps1' -File -Recurse) {
    $parseErrors = $null
    [void][Management.Automation.Language.Parser]::ParseFile($file.FullName, [ref]$null, [ref]$parseErrors)
    if ($parseErrors.Count -gt 0) { throw "$($file.FullName): $($parseErrors.Message -join '; ')" }
}
$pwsh = Join-Path $PSHOME $(if ($IsWindows) { 'pwsh.exe' } else { 'pwsh' })
foreach ($test in Get-ChildItem -LiteralPath (Join-Path $PSScriptRoot 'tests') -Filter '*.tests.ps1' -File | Sort-Object Name) {
    & $pwsh -NoProfile -File $test.FullName
    if ($LASTEXITCODE -ne 0) { throw "$($test.Name) failed with exit code $LASTEXITCODE." }
}
