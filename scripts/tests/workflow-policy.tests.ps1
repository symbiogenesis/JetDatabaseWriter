# Guard shared workflow policy, including future workflows and jobs.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$workflows = @{}
foreach ($file in Get-ChildItem -LiteralPath (Join-Path $root '.github/workflows') -Filter '*.yml' -File) {
    $workflows[$file.Name] = [IO.File]::ReadAllText($file.FullName).Replace("`r`n", "`n")
}

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Policies([hashtable] $Workflows) {
    foreach ($name in $Workflows.Keys) {
        $content = $Workflows[$name]
        Assert-Contract ($content -match '(?m)^  contents: read$' -and $content -notmatch '(?m)^\s+[\w-]+: write$') "$name must keep repository permissions read-only."
        foreach ($step in [regex]::Matches($content, '(?ms)^      - (?<body>.*?)(?=^      - |\z)')) {
            $body = $step.Groups['body'].Value
            foreach ($action in [regex]::Matches($body, '(?m)^(?:uses:|        uses:) (?<action>[^\n]+)$')) {
                Assert-Contract ($action.Groups['action'].Value -match '@[0-9a-f]{40}(?:\s|$)') "$name must pin every action to a commit."
                if ($action.Groups['action'].Value.StartsWith('actions/checkout@', [StringComparison]::Ordinal)) {
                    Assert-Contract ($body -match '(?m)^          persist-credentials: false$') "$name must disable persisted checkout credentials."
                }
            }
        }
        $jobs = [regex]::Match($content, '(?ms)^jobs:\n(?<body>.*)$').Groups['body'].Value
        foreach ($job in [regex]::Matches($jobs, '(?ms)^  [\w-]+:\n(?<body>.*?)(?=^  [\w-]+:\n|\z)')) {
            $body = $job.Groups['body'].Value
            if ($body -match '(?m)^    runs-on:') {
                Assert-Contract ($body -match '(?m)^    timeout-minutes:') "$name must bound every hosted job."
            }
        }
        $concurrency = [regex]::Match($content, '(?ms)^concurrency:\n(?<body>.*?)(?=^\S|\z)').Groups['body'].Value
        if ($concurrency -match '(?m)^  cancel-in-progress: false$') {
            Assert-Contract ($concurrency -match '(?m)^  queue: max$') "$name must preserve queued manual requests."
        }
    }
    $publish = $Workflows['publish.yml']
    Assert-Contract ($publish.Contains('uses: ./.github/workflows/ci.yml') -and $publish.Contains('needs: ci')) 'Publishing must use the tested package after the full reusable CI gate.'
    Assert-Contract ($publish.Contains('runs-on: ubuntu-latest') -and $publish -notmatch '(?m)^\s+(?:run: )?dotnet (?:build|pack|test)\b|^        run: dotnet nuget push[^\n]*--skip-duplicate') 'Publishing must use Linux and upload the tested artifact without rebuilding or hiding duplicate versions.'
}

function Assert-Scenario([string] $Name, [string] $Workflow = '', [string] $Old = '', [string] $New = '', [string] $ExpectedError = '') {
    $candidate = $workflows.Clone()
    if ($Workflow) {
        Assert-Contract ($candidate[$Workflow].Contains($Old)) "The '$Name' negative control must change the workflow."
        $candidate[$Workflow] = $candidate[$Workflow].Replace($Old, $New)
    }
    $caught = ''
    try { Assert-Policies $candidate } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        Assert-Contract ($caught.Contains($ExpectedError)) "$Name expected '$ExpectedError', got '$caught'."
    } else { Assert-Contract (-not $caught) "$Name failed: $caught" }
    "PASS: $Name"
}

Assert-Scenario 'all workflow policies'
Assert-Scenario 'write permission refused' 'ci.yml' 'contents: read' 'contents: write' 'read-only'
Assert-Scenario 'unpinned action refused' 'ci.yml' 'actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1' 'actions/checkout@main' 'pin every action'
Assert-Scenario 'checkout credentials refused' 'ci.yml' 'persist-credentials: false' 'persist-credentials: true' 'disable persisted'
Assert-Scenario 'unbounded publish refused' 'publish.yml' "    timeout-minutes: 10`n" '' 'bound every hosted job'
Assert-Scenario 'manual requests may not be dropped' 'benchmarks.yml' "  queue: max`n" '' 'preserve queued'
Assert-Scenario 'Windows publish refused' 'publish.yml' 'runs-on: ubuntu-latest' 'runs-on: windows-latest' 'Publishing must use Linux'
Assert-Scenario 'release bypass refused' 'publish.yml' 'needs: ci' 'needs: another-job' 'full reusable CI gate'
Assert-Scenario 'release rebuild refused' 'publish.yml' 'run: dotnet nuget push' 'run: dotnet pack' 'Publishing must use Linux'
