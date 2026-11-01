# Automatic campaigns run only when the successful main CI run selected .NET validation.
[CmdletBinding()]
param([string] $SelectionPath = 'artifacts/prerequisite/ci-selection.json')
$ErrorActionPreference = 'Stop'

if ($env:JDW_VALIDATION_SHA -cnotmatch '^[0-9a-f]{40}$') { throw 'Specialized validation requires the exact completed CI revision.' }
if ([string]::IsNullOrWhiteSpace($env:GITHUB_OUTPUT)) { throw 'GITHUB_OUTPUT is required for specialized-validation selection.' }
$run = $true
switch ($env:GITHUB_EVENT_NAME) {
    'workflow_dispatch' { }
    'workflow_run' {
        if (-not [IO.File]::Exists($SelectionPath)) { throw 'The completed CI selection receipt is missing.' }
        $selection = [IO.File]::ReadAllText($SelectionPath) | ConvertFrom-Json -AsHashtable
        if ($selection.revision -cne $env:JDW_VALIDATION_SHA) { throw 'The selection receipt must identify the exact completed CI revision.' }
        if ($selection.dotnet -isnot [bool]) { throw 'The selection receipt must contain a Boolean .NET selection.' }
        $run = $selection.dotnet
    }
    default { throw 'Unsupported specialized-validation trigger.' }
}
"run=$($run.ToString().ToLowerInvariant())" | Add-Content -LiteralPath $env:GITHUB_OUTPUT -Encoding utf8NoBOM
Write-Output "Specialized validation for $($env:JDW_VALIDATION_SHA): $run"
