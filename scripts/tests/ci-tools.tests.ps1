# Keep tool analysis complete without compiling the independently analyzed tests.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$solutionPath = Join-Path $repository 'JetDatabaseWriter.slnx'
$filterPath = Join-Path $repository '.github/ci-tools.slnf'

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

$filter = [IO.File]::ReadAllText($filterPath) | ConvertFrom-Json
$filteredSolution = [IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $filterPath) $filter.solution.path))
Assert-Check ($filteredSolution -eq $solutionPath) 'The tools filter must use the ordinary solution.'
$solution = [xml][IO.File]::ReadAllText($solutionPath)
$separateProjects = @('src/JetDatabaseWriter/JetDatabaseWriter.csproj', 'tests/JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj')
$expected = @($solution.SelectNodes('//Project') | ForEach-Object { $_.Path.Replace('\', '/') } | Where-Object { $_ -notin $separateProjects } | Sort-Object)
$actual = @($filter.solution.projects | ForEach-Object { $_.Replace('\', '/') } | Sort-Object)
Assert-Check ($expected.Count -eq $actual.Count -and (@(Compare-Object $expected $actual -CaseSensitive).Count -eq 0)) 'The tools filter must include every solution project except the separately analyzed library and tests.'
foreach ($project in $actual) {
    Assert-Check (Test-Path -LiteralPath (Join-Path $repository $project) -PathType Leaf) "Missing filtered project: $project"
}
Write-Host 'CI tools filter covers every tool project without compiling the test runner.'
