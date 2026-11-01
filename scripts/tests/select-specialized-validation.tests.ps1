# Automatic expensive campaigns follow the exact successful run's content receipt.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$helper = Join-Path $repository 'scripts/select-specialized-validation.ps1'
$temporary = Join-Path ([IO.Path]::GetTempPath()) ('jdw-specialized-selection-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($temporary)
$variables = @('GITHUB_EVENT_NAME', 'JDW_VALIDATION_SHA', 'GITHUB_OUTPUT')
$saved = @{}
foreach ($name in $variables) { $saved[$name] = [Environment]::GetEnvironmentVariable($name) }

function Assert-Selection([string] $Name, [string] $Event, [object] $Receipt, [string] $Expected, [string] $Error = '') {
    $env:GITHUB_EVENT_NAME = $Event
    $env:JDW_VALIDATION_SHA = 'a' * 40
    $env:GITHUB_OUTPUT = Join-Path $temporary 'output.txt'
    [IO.File]::WriteAllText($env:GITHUB_OUTPUT, '')
    $path = Join-Path $temporary 'ci-selection.json'
    if ($null -eq $Receipt) { Remove-Item -LiteralPath $path -ErrorAction SilentlyContinue }
    else { [IO.File]::WriteAllText($path, ($Receipt | ConvertTo-Json -Depth 5)) }
    $caught = $null
    try { & $helper -SelectionPath $path | Out-Null } catch { $caught = $_.Exception.Message }
    if ($Error) {
        if (-not $caught -or -not $caught.Contains($Error) -or [IO.File]::ReadAllText($env:GITHUB_OUTPUT)) { throw "$Name must refuse without emitting authority: $caught" }
    } elseif ($caught -or [IO.File]::ReadAllText($env:GITHUB_OUTPUT).Trim() -cne "run=$Expected") { throw "$Name produced an incorrect selection: $caught" }
    "PASS: $Name"
}

try {
    Assert-Selection 'manual dispatch remains explicit even without a receipt' 'workflow_dispatch' $null 'true'
    Assert-Selection 'automatic native changes select campaigns' 'workflow_run' @{ revision = 'a' * 40; dotnet = $true } 'true'
    Assert-Selection 'automatic documentation changes skip campaigns' 'workflow_run' @{ revision = 'a' * 40; dotnet = $false; package = $true } 'false'
    Assert-Selection 'automatic proof-only changes skip runtime campaigns' 'workflow_run' @{ revision = 'a' * 40; dotnet = $false; formal = $true } 'false'
    Assert-Selection 'wrong revision cannot authorize skipped campaigns' 'workflow_run' @{ revision = 'b' * 40; dotnet = $false } '' 'exact completed CI revision'
    Assert-Selection 'string boolean cannot authorize skipped campaigns' 'workflow_run' @{ revision = 'a' * 40; dotnet = 'false' } '' 'Boolean .NET selection'
    Assert-Selection 'missing Boolean cannot authorize skipped campaigns' 'workflow_run' @{ revision = 'a' * 40 } '' 'Boolean .NET selection'
    Assert-Selection 'missing receipt refuses automatic campaigns' 'workflow_run' $null '' 'selection receipt is missing'
    Assert-Selection 'unknown trigger refuses' 'pull_request' $null '' 'Unsupported specialized-validation trigger'
}
finally {
    foreach ($name in $variables) { [Environment]::SetEnvironmentVariable($name, $saved[$name]) }
    [IO.Directory]::Delete($temporary, $true)
}
