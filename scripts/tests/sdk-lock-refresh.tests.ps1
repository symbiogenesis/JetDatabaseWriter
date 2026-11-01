# Guard the restore-only hosted patch workflow; no GitHub operations or builds run.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$workflow = [IO.File]::ReadAllText((Join-Path $root '.github/workflows/maintenance.yml')).Replace("`r`n", "`n")

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Workflow([string] $Content) {
    Assert-Contract ($Content -match '(?m)^  workflow_dispatch:$' -and $Content -notmatch '(?m)^  (push|pull_request|schedule):') 'SDK patches must be manually requested.'
    Assert-Contract ($Content -match '(?m)^  contents: read$' -and $Content -notmatch '(?m)^  \w+: write$') 'The refresh workflow must have read-only repository permissions.'
    Assert-Contract ($Content.Contains("if: github.ref == 'refs/heads/main'")) 'The refresh workflow must run only on main.'
    Assert-Contract ($Content.Contains("if (`$env:SDK_VERSION -notmatch '^10\.0\.\d{3}`$')")) 'The workflow must reject floating SDK versions before installation.'
    foreach ($match in [regex]::Matches($Content, '(?m)^\s+uses: (?<action>[^\n]+)$')) {
        Assert-Contract ($match.Groups['action'].Value -match '@[0-9a-f]{40}(?:\s|$)') 'Every refresh workflow action must be pinned.'
    }
    Assert-Contract ($Content.Contains('./scripts/update-sdk.ps1 -Version $env:SDK_VERSION') -and $Content.Contains('SDK_VERSION: ${{ inputs.sdk-version }}')) 'The updater must receive the requested SDK as data.'
    Assert-Contract ($Content.Contains('git diff --binary') -and $Content.Contains('name: sdk-lock-patch') -and $Content.Contains('if-no-files-found: error')) 'The workflow must return a reviewable patch artifact.'
    Assert-Contract ($Content -notmatch '(?m)^\s+(?:run: )?(?:git (?:commit|push|tag)|dotnet (?:build|run|publish|test))\b') 'Lock refresh must not publish or compile.'
}

function Assert-Scenario([string] $Name, [string] $Content, [string] $ExpectedError = '') {
    $caught = ''
    try { Assert-Workflow $Content } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        Assert-Contract ($caught.Contains($ExpectedError)) "$Name expected '$ExpectedError', got '$caught'."
    } else { Assert-Contract (-not $caught) "$Name failed: $caught" }
    "PASS: $Name"
}

Assert-Scenario 'restore-only manual main patch' $workflow
Assert-Scenario 'cannot publish' ($workflow.Replace('contents: read', 'contents: write')) 'read-only repository permissions'
Assert-Scenario 'cannot run a work branch' ($workflow.Replace("if: github.ref == 'refs/heads/main'", 'if: true')) 'only on main'
Assert-Scenario 'cannot float SDK versions' ($workflow.Replace("if (`$env:SDK_VERSION -notmatch '^10\.0\.\d{3}`$')", 'if ($false)')) 'reject floating SDK versions'
Assert-Scenario 'SDK remains data' ($workflow.Replace('-Version $env:SDK_VERSION', '-Version ${{ inputs.sdk-version }}')) 'requested SDK as data'
Assert-Scenario 'no hidden build' ($workflow.Replace('git diff --binary', "dotnet build JetDatabaseWriter.slnx`n          git diff --binary")) 'must not publish or compile'
