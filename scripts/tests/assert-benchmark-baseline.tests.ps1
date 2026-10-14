# Validate benchmark baselines with mocked Git and GitHub; never build or measure anything.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$helper = Join-Path $PSScriptRoot '../assert-benchmark-baseline.ps1'
$headSha = '1111111111111111111111111111111111111111'
$baselineSha = '2222222222222222222222222222222222222222'
$savedToken = $env:GITHUB_TOKEN
$env:GITHUB_TOKEN = 'mock-token'
$context = @{ Calls = [Collections.Generic.List[string]]::new(); Comparison = 'ahead'; Status = 'completed'; Conclusion = 'success'; Resolve = $true; Runs = $true }

function git {
    $context.Calls.Add("git $($args -join ' ')")
    if (($args -join ' ') -notmatch 'rev-parse --verify --end-of-options') { throw 'Unexpected Git operation.' }
    $global:LASTEXITCODE = if ($context.Resolve) { 0 } else { 1 }
    if ($context.Resolve) { $baselineSha }
}

function Invoke-RestMethod {
    param([string] $Uri, $Headers, [string] $Method = 'Get')
    if ($Method -ne 'Get' -or $Headers.Authorization -ne 'Bearer mock-token') { throw 'Expected authenticated read-only requests.' }
    $context.Calls.Add($Uri)
    if ($Uri -eq "https://api.github.com/repos/example/repo/compare/$baselineSha...$headSha") {
        return [pscustomobject]@{ status = $context.Comparison }
    }
    if ($Uri -like "https://api.github.com/repos/example/repo/actions/workflows/ci.yml/runs?branch=main&head_sha=$baselineSha&per_page=100&page=*") {
        $runs = if ($context.Runs) { @([pscustomobject]@{
            id = 42; head_sha = $baselineSha; head_branch = 'main'; event = 'push'; path = '.github/workflows/ci.yml'
            status = $context.Status; conclusion = $context.Conclusion; html_url = 'https://github.com/example/repo/actions/runs/42'
        }) } else { @() }
        return [pscustomobject]@{ total_count = $runs.Count; workflow_runs = $runs }
    }
    throw "Unexpected API request: $Uri"
}

function Assert-Scenario([string] $Name, [string] $ExpectedError = '') {
    $context.Calls.Clear()
    $caught = $null
    $output = ''
    try { $output = (& $helper -Baseline 'published-baseline' -HeadSha $headSha -Repo 'example/repo' | Out-String).Trim() }
    catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or -not $caught.Contains($ExpectedError)) { throw "$Name expected '$ExpectedError', got '$caught'." }
    } elseif ($caught -or $output -ne $baselineSha) {
        throw "$Name expected only the immutable baseline SHA; got '$output', error '$caught'."
    }
    "PASS: $Name"
}

try {
    Assert-Scenario 'published ancestor with successful exact CI'
    $context.Comparison = 'identical'
    Assert-Scenario 'identical comparison revision'
    foreach ($comparison in 'behind', 'diverged') {
        $context.Comparison = $comparison
        Assert-Scenario "$comparison baseline rejected" 'published main history'
        if ($context.Calls -match '/workflows/') { throw 'Unpublished baseline queried CI.' }
    }
    $context.Comparison = 'ahead'
    $context.Resolve = $false
    Assert-Scenario 'unresolved reference rejected' 'Cannot resolve'
    if ($context.Calls.Count -ne 1) { throw 'Unresolved reference reached GitHub.' }
    $context.Resolve = $true
    $context.Runs = $false
    Assert-Scenario 'missing baseline CI rejected' 'No CI run'
    $context.Runs = $true
    $context.Status = 'in_progress'
    Assert-Scenario 'pending baseline CI rejected without polling' 'has not passed'
    if (@($context.Calls -match '/workflows/').Count -ne 1) { throw 'Pending baseline CI was polled.' }
    $context.Status = 'completed'
    $context.Conclusion = 'failure'
    Assert-Scenario 'failed baseline CI rejected' 'has not passed'

    $workflow = [IO.File]::ReadAllText((Join-Path $PSScriptRoot '../../.github/workflows/benchmarks.yml')).Replace("`r`n", "`n")
    $prerequisite = [regex]::Match($workflow, '(?ms)^  prerequisite:\n(?<body>.*?)(?=^  benchmarks:)').Groups['body'].Value
    if (-not $prerequisite.Contains('./scripts/assert-benchmark-baseline.ps1') -or
        -not $prerequisite.Contains('fetch-depth: 0') -or
        -not $prerequisite.Contains('sha=${baselineSha}') -or
        -not $prerequisite.Contains('baseline: ${{ steps.baseline.outputs.sha }}')) {
        throw 'The prerequisite must validate and export the immutable baseline SHA before builds.'
    }
    $checkout = [regex]::Match($workflow, '(?ms)^      - name: Check out the baseline\n(?<body>.*?)(?=^      - name:)').Groups['body'].Value
    if (-not $checkout.Contains('BASELINE_SHA: ${{ needs.prerequisite.outputs.baseline }}') -or
        $checkout.Contains('inputs.baseline') -or $checkout.Contains('rev-parse')) {
        throw 'Baseline checkout must use the already validated immutable SHA.'
    }
    $sdk = [regex]::Match($workflow, '(?ms)^      - name: Setup baseline SDK\n(?<body>.*?)(?=^      - name:)').Groups['body'].Value
    if (-not $sdk.Contains('global-json-file: ${{ runner.temp }}/baseline/global.json') -or
        -not $sdk.Contains("if: needs.prerequisite.outputs.baseline != ''") -or
        $workflow.IndexOf('name: Setup baseline SDK', [StringComparison]::Ordinal) -lt $workflow.IndexOf('name: Check out the baseline', [StringComparison]::Ordinal) -or
        $workflow.IndexOf('name: Setup baseline SDK', [StringComparison]::Ordinal) -gt $workflow.IndexOf('name: Build the baseline', [StringComparison]::Ordinal)) {
        throw 'Validated baselines must have their own pinned SDK installed before their build.'
    }
    if (-not $prerequisite.Contains("if: github.ref == 'refs/heads/main'")) { throw 'Benchmark dispatch must use main.' }
    'PASS: hosted baseline validation and immutable checkout contract'
}
finally { $env:GITHUB_TOKEN = $savedToken }
