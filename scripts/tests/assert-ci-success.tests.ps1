# Exercises the hosted prerequisite with synthetic API responses; no builds or network calls run.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$helper = Join-Path $PSScriptRoot '../assert-ci-success.ps1'
$sha = '1111111111111111111111111111111111111111'
$otherSha = '2222222222222222222222222222222222222222'
$savedToken = $env:GITHUB_TOKEN
$env:GITHUB_TOKEN = 'mock-token'
$ciPrerequisiteContext = @{ Calls = [Collections.Generic.List[string]]::new(); Runs = @(); ApiFailure = $false }

function New-Run([long] $Id = 42, [string] $Status = 'completed', [string] $Conclusion = 'success') {
    [pscustomobject]@{
        id = $Id; head_sha = $sha; head_branch = 'main'; event = 'push'
        path = '.github/workflows/ci.yml'; status = $Status; conclusion = $Conclusion
        html_url = "https://github.com/example/repo/actions/runs/$Id"
    }
}

function Invoke-RestMethod {
    param([string] $Uri, $Headers, [string] $Method = 'Get')
    if ($Method -ne 'Get') { throw 'The prerequisite must not dispatch or rerun CI.' }
    if ($Headers.Authorization -ne 'Bearer mock-token') { throw 'Missing authenticated API request.' }
    if ($Uri -notlike "https://api.github.com/repos/example/repo/actions/workflows/ci.yml/runs?branch=main&head_sha=$sha&per_page=100&page=*") {
        throw "Unexpected API request: $Uri"
    }
    $ciPrerequisiteContext.Calls.Add($Uri)
    if ($ciPrerequisiteContext.ApiFailure) { throw 'Mocked API failure.' }
    $page = [int]($Uri -replace '^.*&page=', '')
    [pscustomobject]@{
        total_count = $ciPrerequisiteContext.Runs.Count
        workflow_runs = @($ciPrerequisiteContext.Runs | Select-Object -Skip (($page - 1) * 100) -First 100)
    }
}

function Assert-Scenario([string] $Name, [object[]] $Runs, [string] $ExpectedError = '', [string] $ExpectedRun = '/42') {
    $ciPrerequisiteContext.Calls.Clear()
    $ciPrerequisiteContext.Runs = $Runs
    $caught = $null
    $output = ''
    try { $output = (& $helper -Sha $sha -Repo 'example/repo' 6>&1 | Out-String) }
    catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or $caught -notlike "*$ExpectedError*") { throw "$Name expected '$ExpectedError', got '$caught'." }
    } elseif ($caught) {
        throw "$Name failed: $caught"
    } elseif ($output -notlike "*$sha*" -or $output -notlike "*$ExpectedRun*") {
        throw "$Name did not report the accepted revision and run."
    }
    if ($output.Contains('mock-token') -or ($caught -and $caught.Contains('mock-token'))) { throw "$Name exposed the token." }
    "PASS: $Name"
}

try {
    Assert-Scenario 'completed exact main CI succeeds' @((New-Run))
    $manual = New-Run
    $manual.event = 'workflow_dispatch'
    $manual.path += '@main'
    Assert-Scenario 'manual CI with workflow path suffix succeeds' @($manual)
    Assert-Scenario 'missing CI is refused' @() 'No CI run'
    foreach ($status in 'queued', 'in_progress', 'waiting') {
        Assert-Scenario "$status CI is refused without polling" @((New-Run -Status $status -Conclusion '')) 'has not passed'
        if ($ciPrerequisiteContext.Calls.Count -ne 1) { throw 'Pending CI must not be polled.' }
    }
    foreach ($conclusion in 'failure', 'cancelled', 'timed_out', 'skipped', 'neutral') {
        Assert-Scenario "$conclusion CI is refused" @((New-Run -Conclusion $conclusion)) 'has not passed'
    }
    Assert-Scenario 'new failed run supersedes older success' @((New-Run -Id 41), (New-Run -Conclusion 'failure')) 'has not passed'
    Assert-Scenario 'new pending run supersedes older success' @((New-Run -Id 41), (New-Run -Status 'in_progress' -Conclusion '')) 'has not passed'
    Assert-Scenario 'new completed success supersedes older failure' @((New-Run -Id 41 -Conclusion 'failure'), (New-Run))
    foreach ($property in 'head_sha', 'head_branch', 'path', 'event') {
        $wrong = New-Run
        $wrong.$property = switch ($property) {
            'head_sha' { $otherSha }
            'head_branch' { 'work' }
            'path' { '.github/workflows/benchmarks.yml' }
            'event' { 'pull_request' }
        }
        Assert-Scenario "mismatched $property cannot certify CI" @($wrong) 'No CI run'
    }
    $wrongNew = New-Run -Id 43
    $wrongNew.event = 'pull_request'
    Assert-Scenario 'unrelated newer run cannot replace main CI' @((New-Run), $wrongNew)
    $olderSuccess = New-Run -Id 1
    $manyRuns = @(2..102 | ForEach-Object { New-Run -Id $_ -Conclusion 'failure' })
    $manyRuns += $olderSuccess
    Assert-Scenario 'pagination cannot select an old success' $manyRuns 'has not passed'
    if ($ciPrerequisiteContext.Calls.Count -ne 2) { throw 'CI discovery did not inspect every response page.' }
    $ciPrerequisiteContext.ApiFailure = $true
    Assert-Scenario 'API errors fail closed' @((New-Run)) 'Mocked API failure'
    $ciPrerequisiteContext.ApiFailure = $false
    foreach ($invalid in @(@{ Sha = 'short'; Repo = 'example/repo' }, @{ Sha = $sha; Repo = 'example/repo/extra' })) {
        $ciPrerequisiteContext.Calls.Clear()
        $caught = $null
        try { & $helper @invalid } catch { $caught = $_ }
        if (-not $caught -or $ciPrerequisiteContext.Calls.Count -ne 0) { throw 'Invalid input reached the API.' }
    }
    $env:GITHUB_TOKEN = ''
    Assert-Scenario 'missing token is refused' @((New-Run)) 'GITHUB_TOKEN'
    if ($ciPrerequisiteContext.Calls.Count -ne 0) { throw 'Unauthenticated discovery reached the API.' }
}
finally { $env:GITHUB_TOKEN = $savedToken }
