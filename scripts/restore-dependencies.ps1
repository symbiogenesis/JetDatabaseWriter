<#
.SYNOPSIS
Restores the solution and all SDK-dependent smoke lockfiles without compiling.
.DESCRIPTION
Restoration is locked by default. Refresh is reserved for intentional SDK/package updates;
it reevaluates both the solution and the seed, trimmed and native consumer configurations.
#>
[CmdletBinding()]
param(
    [switch] $Refresh,
    [switch] $AotOnly
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = Split-Path -Parent $PSScriptRoot
[string[]] $options = if ($Refresh) { @('--force-evaluate', '-p:RestoreLockedMode=false') } else { @('--locked-mode') }

function Invoke-Restore([string[]] $Arguments) {
    & dotnet restore @Arguments @options
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet restore $($Arguments -join ' ') failed with exit code $LASTEXITCODE."
    }
}

Push-Location $repository
try {
    if (-not $AotOnly) {
        Invoke-Restore @('JetDatabaseWriter.slnx')
        Invoke-Restore @('tests/JetDatabaseWriter.Tests', '-p:TestNetStandard=true')
    }
    foreach ($mode in 'seed', 'trimmed', 'native') {
        Invoke-Restore @('scripts/AotSmoke/AotSmoke.csproj', "-p:SmokeMode=$mode")
    }
}
finally {
    Pop-Location
}
