# Exercise release evidence with synthetic GitHub responses and archives only.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$helper = Join-Path $PSScriptRoot '../assert-release-readiness.ps1'
$sha = '1111111111111111111111111111111111111111'
$mainSha = '2222222222222222222222222222222222222222'
$savedToken = $env:GITHUB_TOKEN
$env:GITHUB_TOKEN = 'mock-token'
$context = @{ Calls = [Collections.Generic.List[string]]::new() }

function New-Run([string] $Kind, [long] $Id) {
    [pscustomobject]@{
        id = $Id; run_attempt = 1; workflow_id = @{ ci = 1; fuzz = 2; portability = 3 }[$Kind]
        head_sha = $sha; head_branch = 'main'; event = $(if ($Kind -eq 'ci') { 'push' } else { 'workflow_run' })
        path = ".github/workflows/$Kind.yml"; status = 'completed'; conclusion = 'success'
        display_title = "Validation: $Kind $sha"
        repository = [pscustomobject]@{ full_name = 'example/repo' }
        head_repository = [pscustomobject]@{ full_name = 'example/repo' }
        html_url = "https://github.com/example/repo/actions/runs/$Id"
    }
}

function Reset-Context {
    $context.Calls.Clear()
    $context.Comparison = 'ahead'
    $context.MergeBase = $sha
    $context.ApiFailure = $false
    $context.ArtifactMissing = $false
    $context.ArtifactExpired = $false
    $context.Runs = @{ ci = @((New-Run ci 10)); fuzz = @((New-Run fuzz 20)); portability = @((New-Run portability 30)) }
    $context.Jobs = @{
        ci = @('Select CI lanes', 'Script, security and proof review checks', 'CI gate')
        fuzz = @('Require successful CI', 'Fuzz (net10.0)', 'Fuzz (net11.0)', 'Record release evidence')
        portability = @('Require successful CI', 'Test (ubuntu-latest, net10.0)', 'Test (ubuntu-latest, net11.0)', 'Test (macos-latest, net10.0)', 'Test (macos-latest, net11.0)', 'Record release evidence')
    }
    $context.JobStatus = 'completed'
    $context.JobConclusion = 'success'
    $context.ReceiptOverride = @{}
    $context.ArtifactRunOverride = $null
    $context.CountOverride = $null
    $context.DuplicateArtifact = $false
    $context.ArchiveEntryName = 'receipt.json'
    $context.ArtifactSize = 512
    $context.EnforceSearchCap = $false
    $context.ForgeEntryLength = $false
}

function Invoke-RestMethod {
    param([string] $Uri, $Headers, [string] $Method = 'Get', [int] $TimeoutSec)
    if ($Method -ne 'Get' -or $Headers.Authorization -ne 'Bearer mock-token') { throw 'Expected authenticated read-only API requests.' }
    if ($TimeoutSec -ne 60) { throw 'Expected a bounded evidence API timeout.' }
    $context.Calls.Add($Uri)
    if ($context.ApiFailure) { throw 'Synthetic API failure.' }
    if ($Uri -eq 'https://api.github.com/repos/example/repo/git/ref/heads/main') { return @{ object = @{ sha = $mainSha } } }
    if ($Uri -eq "https://api.github.com/repos/example/repo/compare/$sha...$mainSha") {
        return @{ status = $context.Comparison; merge_base_commit = @{ sha = $context.MergeBase } }
    }
    if ($Uri -match '/actions/workflows/(ci|fuzz|portability)\.yml$') {
        $kind = $Matches[1]
        return @{ id = @{ ci = 1; fuzz = 2; portability = 3 }[$kind]; path = ".github/workflows/$kind.yml" }
    }
    if ($Uri -match '/actions/workflows/(ci|fuzz|portability)\.yml/runs\?.*page=(\d+)$') {
        $kind = $Matches[1]; $page = [int]$Matches[2]
        $runs = @($context.Runs[$kind])
        if ($context.EnforceSearchCap -and $Uri.Contains('branch=main')) { $runs = @($runs | Select-Object -First 1000) }
        return @{ total_count = $(if ($null -ne $context.CountOverride) { $context.CountOverride } else { $runs.Count }); workflow_runs = @($runs | Select-Object -Skip (($page - 1) * 100) -First 100) }
    }
    if ($Uri -match '/actions/runs/(\d+)/jobs\?filter=latest&per_page=100&page=(\d+)$') {
        $runId = [long]$Matches[1]; $page = [int]$Matches[2]
        $kind = switch ($runId) { 10 { 'ci' }; 20 { 'fuzz' }; 30 { 'portability' }; default { throw "Unexpected run $runId." } }
        $jobs = @($context.Jobs[$kind] | ForEach-Object { @{ name = $_; status = $context.JobStatus; conclusion = $context.JobConclusion } })
        return @{ total_count = $jobs.Count; jobs = @($jobs | Select-Object -Skip (($page - 1) * 100) -First 100) }
    }
    if ($Uri -match '/actions/runs/(20|30)/artifacts\?per_page=100&page=(\d+)$') {
        $runId = [long]$Matches[1]
        $kind = if ($runId -eq 20) { 'fuzz' } else { 'portability' }
        $artifacts = @(if (-not $context.ArtifactMissing) { @{
            id = $runId; name = "release-evidence-$kind-$sha"; expired = $context.ArtifactExpired
            size_in_bytes = $context.ArtifactSize
            archive_download_url = "https://api.github.com/repos/example/repo/actions/artifacts/$runId/zip"
            workflow_run = @{ id = $(if ($context.ArtifactRunOverride) { $context.ArtifactRunOverride } else { $runId }) }
        } })
        if ($context.DuplicateArtifact) { $artifacts += $artifacts[0] }
        return @{ total_count = $artifacts.Count; artifacts = $artifacts }
    }
    throw "Unexpected API request: $Uri"
}

function Invoke-WebRequest {
    param([string] $Uri, $Headers, [string] $OutFile, [string] $Method = 'Get', [int] $TimeoutSec)
    if ($Method -ne 'Get' -or $Headers.Authorization -ne 'Bearer mock-token') { throw 'Expected authenticated artifact download.' }
    if ($TimeoutSec -ne 60) { throw 'Expected a bounded receipt download timeout.' }
    $context.Calls.Add($Uri)
    if ($Uri -notmatch '^https://api.github.com/repos/example/repo/actions/artifacts/(20|30)/zip$') { throw "Unexpected archive request: $Uri" }
    $runId = [long]$Matches[1]
    $kind = if ($runId -eq 20) { 'fuzz' } else { 'portability' }
    $receipt = @{ schemaVersion = 1; repository = 'example/repo'; workflowPath = ".github/workflows/$kind.yml"; revision = $sha; runId = $runId; runAttempt = 1 }
    foreach ($key in $context.ReceiptOverride.Keys) { $receipt[$key] = $context.ReceiptOverride[$key] }
    $archive = [IO.Compression.ZipFile]::Open($OutFile, [IO.Compression.ZipArchiveMode]::Create)
    try {
        $entry = $archive.CreateEntry($context.ArchiveEntryName)
        $writer = [IO.StreamWriter]::new($entry.Open())
        try {
            $writer.Write(($receipt | ConvertTo-Json))
            if ($context.ForgeEntryLength) { $writer.Write((' ' * 20000)) }
        } finally { $writer.Dispose() }
    } finally { $archive.Dispose() }
    if ($context.ForgeEntryLength) {
        $bytes = [IO.File]::ReadAllBytes($OutFile)
        $forgedLength = [BitConverter]::GetBytes([int]1)
        [Array]::Copy($forgedLength, 0, $bytes, 22, 4)
        for ($index = 0; $index -le $bytes.Length - 28; $index++) {
            if ($bytes[$index] -eq 0x50 -and $bytes[$index + 1] -eq 0x4b -and $bytes[$index + 2] -eq 1 -and $bytes[$index + 3] -eq 2) {
                [Array]::Copy($forgedLength, 0, $bytes, $index + 24, 4)
                break
            }
        }
        [IO.File]::WriteAllBytes($OutFile, $bytes)
    }
}

function Assert-Scenario([string] $Name, [string] $ExpectedError = '') {
    $context.Calls.Clear()
    $caught = $null
    $output = ''
    try { $output = & $helper -Sha $sha -Repo 'example/repo' | Out-String }
    catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or -not $caught.Contains($ExpectedError)) { throw "$Name expected '$ExpectedError', got '$caught'." }
    } elseif ($caught -or $output -notlike "*$sha*" -or $output -notlike '*/runs/20*' -or $output -notlike '*/runs/30*') {
        throw "$Name failed: '$caught'; output '$output'."
    }
    if ($output.Contains('mock-token') -or ($caught -and $caught.Contains('mock-token'))) { throw "$Name exposed credentials." }
    "PASS: $Name"
}

try {
    Reset-Context
    Assert-Scenario 'published ancestor accepts complete exact-SHA receipts'
    if (@($context.Calls | Where-Object { $_ -like "*/ci.yml/runs?per_page=100&branch=main&head_sha=$sha&page=1" }).Count -ne 1) {
        throw 'Main CI discovery must retain its exact-SHA server filter.'
    }
    $context.Comparison = 'identical'
    Assert-Scenario 'published main tip accepts the same evidence'
    foreach ($comparison in 'behind', 'diverged') {
        Reset-Context; $context.Comparison = $comparison
        Assert-Scenario "$comparison commit is refused" 'published main history'
    }
    Reset-Context; $context.MergeBase = $mainSha
    Assert-Scenario 'contradictory ancestry is refused' 'published main history'
    foreach ($kind in 'ci', 'fuzz', 'portability') {
        Reset-Context; $context.Runs[$kind] = @()
        Assert-Scenario "missing $kind is refused" 'No eligible'
        foreach ($status in 'queued', 'in_progress', 'waiting') {
            Reset-Context; $context.Runs[$kind][0].status = $status; $context.Runs[$kind][0].conclusion = ''
            Assert-Scenario "$status $kind is refused" 'has not passed'
        }
        foreach ($conclusion in 'failure', 'cancelled', 'timed_out', 'skipped', 'neutral') {
            Reset-Context; $context.Runs[$kind][0].conclusion = $conclusion
            Assert-Scenario "$conclusion $kind is refused" 'has not passed'
        }
        Reset-Context
        $newer = New-Run $kind 99; $newer.conclusion = 'failure'
        $context.Runs[$kind] += $newer
        Assert-Scenario "newer failed $kind supersedes green evidence" 'has not passed'
        $newer.status = 'queued'; $newer.conclusion = ''
        Assert-Scenario "newer pending $kind supersedes green evidence" 'has not passed'
        foreach ($property in 'head_repository', 'repository', 'workflow_id', 'path', 'head_branch', 'event') {
            Reset-Context
            $run = $context.Runs[$kind][0]
            $run.$property = switch ($property) {
                'head_repository' { @{ full_name = 'fork/repo' } }; 'repository' { @{ full_name = 'other/repo' } }
                'workflow_id' { 999 }; 'path' { '.github/workflows/unrelated.yml' }
                'head_branch' { 'work' }; 'event' { 'pull_request' }
            }
            Assert-Scenario "$kind mismatched $property is refused" 'No eligible'
        }
        Reset-Context; $context.Jobs[$kind] = @($context.Jobs[$kind] | Select-Object -Skip 1)
        Assert-Scenario "$kind missing required job is refused" 'inventory'
        Reset-Context; $context.Jobs[$kind] += $context.Jobs[$kind][0]
        Assert-Scenario "$kind duplicate job is refused" 'inventory'
        Reset-Context; $context.Jobs[$kind] += 'Unreviewed new job'
        Assert-Scenario "$kind unknown job is refused" 'inventory'
    }
    Reset-Context
    $context.Runs.fuzz[0].head_sha = $mainSha
    Assert-Scenario 'workflow-run receipt binds target when main has advanced'
    Reset-Context; $context.Runs.fuzz[0].display_title = "Validation: fuzz $mainSha"
    Assert-Scenario 'different explicit campaign target is refused' 'No eligible'
    foreach ($conclusion in 'skipped', 'failure', 'cancelled') {
        Reset-Context; $context.JobConclusion = $conclusion
        Assert-Scenario "$conclusion inventory is refused" 'inventory'
    }
    Reset-Context; $context.ArtifactMissing = $true
    Assert-Scenario 'missing success receipt is refused' 'receipt artifact'
    Reset-Context; $context.ArtifactExpired = $true
    Assert-Scenario 'expired success receipt is refused' 'receipt artifact'
    Reset-Context; $context.ArtifactRunOverride = 999
    Assert-Scenario 'artifact from another run is refused' 'receipt artifact'
    Reset-Context; $context.DuplicateArtifact = $true
    Assert-Scenario 'duplicate receipt artifact is refused' 'receipt artifact'
    Reset-Context; $context.ArtifactSize = 1048577
    Assert-Scenario 'oversized receipt download is refused' 'receipt artifact'
    Reset-Context; $context.ArchiveEntryName = '../receipt.json'
    Assert-Scenario 'noncanonical archive entry is refused without extraction' 'receipt archive'
    Reset-Context; $context.ForgeEntryLength = $true
    Assert-Scenario 'forged ZIP length cannot bypass the decoded receipt limit' 'receipt archive'
    Reset-Context; $context.Runs.fuzz[0].run_attempt = 2
    Assert-Scenario 'previous attempt receipt cannot certify a rerun' 'receipt does not match'
    foreach ($key in 'schemaVersion', 'repository', 'workflowPath', 'revision', 'runId', 'runAttempt') {
        Reset-Context
        $context.ReceiptOverride[$key] = switch ($key) {
            'schemaVersion' { 2 }; 'repository' { 'fork/repo' }; 'workflowPath' { '.github/workflows/wrong.yml' }
            'revision' { $mainSha }; 'runId' { 999 }; 'runAttempt' { 2 }
        }
        Assert-Scenario "receipt mismatched $key is refused" 'receipt does not match'
    }
    Reset-Context; $context.ReceiptOverride.schemaVersion = $true
    Assert-Scenario 'Boolean receipt schema is refused' 'receipt does not match'
    Reset-Context; $context.ReceiptOverride.runId = '20'
    Assert-Scenario 'string run identity is refused' 'receipt does not match'
    Reset-Context
    $context.Runs.fuzz = @(1..101 | ForEach-Object { $run = New-Run fuzz (100 + $_); $run.display_title = "Validation: fuzz $mainSha"; $run.head_sha = $mainSha; $run }) + $context.Runs.fuzz
    Assert-Scenario 'run discovery paginates before selecting matching campaign'
    if (@($context.Calls | Where-Object { $_ -match '/fuzz.yml/runs\?.*&page=2$' }).Count -ne 1) { throw 'Campaign discovery omitted the second page.' }
    Reset-Context
    $context.EnforceSearchCap = $true
    $context.Runs.fuzz = @(1..1001 | ForEach-Object { $run = New-Run fuzz (100 + $_); $run.display_title = "Validation: fuzz $mainSha"; $run.head_sha = $mainSha; $run }) + $context.Runs.fuzz
    Assert-Scenario 'campaign discovery avoids the filtered thousand-run API cap'
    if (@($context.Calls | Where-Object { $_ -match '/fuzz.yml/runs\?[^\r\n]*branch=' }).Count -ne 0) { throw 'Campaign discovery invoked the capped branch search.' }
    Reset-Context; $context.CountOverride = 2
    Assert-Scenario 'truncated API inventory is refused' 'Incomplete GitHub evidence'
    Reset-Context; $context.ApiFailure = $true
    Assert-Scenario 'API errors fail closed' 'Synthetic API failure'
    foreach ($arguments in @(@{ Sha = 'short'; Repo = 'example/repo' }, @{ Sha = $sha; Repo = 'example/repo/extra' })) {
        Reset-Context
        $caught = $null
        try { & $helper @arguments } catch { $caught = $_ }
        if (-not $caught -or $context.Calls.Count -ne 0) { throw 'Invalid release inputs reached the API.' }
    }
    Reset-Context; $env:GITHUB_TOKEN = ''
    Assert-Scenario 'missing token is refused' 'GITHUB_TOKEN'
    if ($context.Calls.Count -ne 0) { throw 'Missing credentials reached the API.' }
}
finally {
    $env:GITHUB_TOKEN = $savedToken
}
