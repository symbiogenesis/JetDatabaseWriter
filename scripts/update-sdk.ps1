<#
.SYNOPSIS
Updates the exact SDK pin and every restore lockfile together, without compiling.
.DESCRIPTION
Install the requested SDK first, or dispatch maintenance.yml with task=sdk-lock-refresh for a hosted patch.
If any refresh or locked verification fails, the original global.json and lockfile bytes
are restored. Successful changes stay uncommitted for review and the ordinary main CI gate.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^10\.0\.\d{3}$')]
    [string] $Version
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = Split-Path -Parent $PSScriptRoot
$globalPath = Join-Path $repository 'global.json'
$configuration = [IO.File]::ReadAllText($globalPath)
$globalJson = $configuration | ConvertFrom-Json
if ($globalJson.sdk.rollForward -ne 'disable') { throw 'SDK updates require an exact pin with rollForward disabled.' }
$solution = [xml][IO.File]::ReadAllText((Join-Path $repository 'JetDatabaseWriter.slnx'))
$files = [Collections.Generic.List[string]]::new()
$files.Add($globalPath)
foreach ($project in $solution.SelectNodes('//Project[@Path]')) {
    $projectPath = [IO.Path]::GetFullPath((Join-Path $repository $project.Path))
    if (-not $projectPath.StartsWith($repository + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Solution project must stay inside the repository: $($project.Path)"
    }
    $files.Add((Join-Path (Split-Path -Parent $projectPath) 'packages.lock.json'))
    foreach ($lock in Get-ChildItem -LiteralPath (Split-Path -Parent $projectPath) -Filter 'packages.netstandard.lock.json' -File) {
        $files.Add($lock.FullName)
    }
}
foreach ($mode in 'seed', 'trimmed', 'native') {
    $files.Add((Join-Path $PSScriptRoot "AotSmoke/packages.$mode.lock.json"))
}
$originals = @{}
foreach ($file in $files | Select-Object -Unique) {
    if ([IO.File]::Exists($file)) {
        $originals[$file] = [IO.File]::ReadAllBytes($file)
    } else { $originals[$file] = $null }
}

Push-Location $repository
try {
    # Preserve the established JSON formatting and all settings outside the SDK version.
    $versionPattern = '("version"\s*:\s*")' + [regex]::Escape($globalJson.sdk.version) + '(")'
    if ([regex]::Matches($configuration, $versionPattern).Count -ne 1) { throw 'global.json must contain exactly one SDK version pin.' }
    $replacement = [Text.RegularExpressions.MatchEvaluator] { param($match) $match.Groups[1].Value + $Version + $match.Groups[2].Value }
    [IO.File]::WriteAllText($globalPath, [regex]::Replace($configuration, $versionPattern, $replacement), [Text.UTF8Encoding]::new($false))
    $selected = (& dotnet --version | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $selected -ne $Version) {
        throw "The selected SDK is '$selected'; install SDK $Version before updating its locks."
    }
    & (Join-Path $PSScriptRoot 'restore-dependencies.ps1') -Refresh
    & (Join-Path $PSScriptRoot 'restore-dependencies.ps1')
    foreach ($file in $originals.Keys) {
        if ([IO.File]::Exists($file)) {
            $content = [regex]::Replace([IO.File]::ReadAllText($file), '(?<!\r)\n', "`r`n")
            [IO.File]::WriteAllText($file, $content, [Text.UTF8Encoding]::new($false))
        }
    }
    Write-Host "SDK $Version and all solution/smoke locks refreshed and verified. Review the diff, then publish main for CI."
}
catch {
    foreach ($file in $originals.Keys) {
        if ($null -eq $originals[$file]) {
            if ([IO.File]::Exists($file)) { [IO.File]::Delete($file) }
        } else { [IO.File]::WriteAllBytes($file, $originals[$file]) }
    }
    throw
}
finally {
    Pop-Location
}
