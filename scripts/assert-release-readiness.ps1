<#
.SYNOPSIS
Requires exact-revision main CI and completed fuzz/portability evidence before publication.
.DESCRIPTION
Reads existing GitHub evidence only. A newer pending or unsuccessful campaign supersedes
older success. Explicit campaign titles and success receipts bind workflow_run executions
to their actual target even when published main advances. No work is dispatched or polled.
#>
[CmdletBinding()]
param(
    [string] $Sha = $env:GITHUB_SHA,
    [string] $Repo = $env:GITHUB_REPOSITORY
)
$ErrorActionPreference = 'Stop'
if ($Sha -cnotmatch '^[0-9a-f]{40}$') { throw 'Sha must be a full lowercase 40-character commit ID.' }
if ($Repo -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') { throw 'Repo must be owner/name.' }
if ([string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) { throw 'GITHUB_TOKEN is required to read release evidence.' }
$headers = @{
    Authorization = "Bearer $env:GITHUB_TOKEN"
    Accept = 'application/vnd.github+json'
    'User-Agent' = 'jdw-release-readiness'
    'X-GitHub-Api-Version' = '2026-03-10'
}
$api = "https://api.github.com/repos/$Repo"
. "$PSScriptRoot/ci-matrix.ps1"

function Get-ReleasePages([string] $Uri, [string] $Property) {
    $items = [Collections.Generic.List[object]]::new()
    $total = $null
    for ($page = 1; ; $page++) {
        $response = Invoke-RestMethod -Uri "$Uri&page=$page" -Headers $headers -TimeoutSec 60
        $batch = @($response.$Property)
        if ($null -eq $response.total_count -or $response.total_count -lt 0 -or $batch.Count -gt 100 -or
            ($null -ne $total -and $response.total_count -ne $total)) { throw 'Incomplete GitHub evidence response.' }
        $total = $response.total_count
        foreach ($item in $batch) { $items.Add($item) }
        if ($page * 100 -ge $total) {
            if ($items.Count -ne $total) { throw "Incomplete GitHub evidence inventory: $($items.Count)/$total at $Uri." }
            break
        }
        if ($batch.Count -ne 100) { throw 'Incomplete GitHub evidence pagination.' }
    }
    return $items.ToArray()
}

function Get-ReleaseRun([string] $Kind) {
    $workflow = Invoke-RestMethod -Uri "$api/actions/workflows/$Kind.yml" -Headers $headers -TimeoutSec 60
    if ($workflow.id -le 0 -or $workflow.path -cne ".github/workflows/$Kind.yml") { throw "Unexpected $Kind workflow identity." }
    # GitHub caps filtered run searches at 1,000 results. Campaign target titles
    # require local filtering, so enumerate their uncapped workflow history.
    $uri = "$api/actions/workflows/$Kind.yml/runs?per_page=100"
    if ($Kind -eq 'ci') { $uri += "&branch=main&head_sha=$Sha" }
    $runs = @(Get-ReleasePages $uri 'workflow_runs')
    $matching = @($runs | Where-Object {
        $run = $_
        $official = $run.repository.full_name -ieq $Repo -and $run.head_repository.full_name -ieq $Repo -and
            $run.workflow_id -eq $workflow.id -and ($run.path -split '@', 2)[0] -ceq $workflow.path -and
            $run.head_branch -ceq 'main' -and $run.id -gt 0 -and $run.run_attempt -gt 0
        if ($Kind -eq 'ci') {
            $official -and $run.head_sha -ceq $Sha -and $run.event -cin 'push', 'workflow_dispatch'
        } else {
            $official -and $run.display_title -ceq "Validation: $Kind $Sha" -and $run.event -cin 'workflow_run', 'workflow_dispatch'
        }
    })
    $latest = $matching | Sort-Object { [long]$_.id } -Descending | Select-Object -First 1
    if (-not $latest) { throw "No eligible $Kind run for exact main revision $Sha." }
    if ($latest.status -cne 'completed' -or $latest.conclusion -cne 'success') {
        throw "$Kind for revision $Sha has not passed: status '$($latest.status)', conclusion '$($latest.conclusion)'. https://github.com/$Repo/actions/runs/$($latest.id)"
    }
    return $latest
}

function Assert-ReleaseJobs($Run, [string] $Kind) {
    $jobs = @(Get-ReleasePages "$api/actions/runs/$($Run.id)/jobs?filter=latest&per_page=100" 'jobs')
    $required = switch ($Kind) {
        'ci' { 'Select CI lanes'; 'Script, security and proof review checks'; 'CI gate' }
        'fuzz' { 'Require successful CI'; 'Fuzz (net10.0)'; 'Fuzz (net11.0)'; 'Record release evidence' }
        'portability' {
            'Require successful CI'
            foreach ($os in 'ubuntu-latest', 'macos-latest') {
                foreach ($tf in 'net10.0', 'net11.0') { "Test ($os, $tf)" }
            }
            'Record release evidence'
        }
    }
    $names = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $optionalCiJobs = @(
        foreach ($entry in (Get-CiAnalyzerMatrix).include) { "Analyze ($($entry.name))" }
        foreach ($entry in (Get-CiTestMatrix).include) { "Test ($($entry.name))" }
        'Verify recovery models (Dafny)', 'Package documentation', 'Trimmed and NativeAOT consumers'
        foreach ($tf in 'net10.0', 'net10.0-netstandard', 'net11.0') { "Native Access compatibility ($tf)" }
        foreach ($os in 'ubuntu-latest', 'macos-latest') {
            foreach ($tf in 'net10.0', 'net11.0') { "Encryption replacement ($os, $tf)" }
        }
    )
    foreach ($job in $jobs) {
        if (-not $names.Add($job.name) -or $job.status -cne 'completed') { throw "Invalid $Kind job inventory." }
        if ($Kind -eq 'ci' -and $job.name -cnotin $required) {
            # The aggregate CI gate checks the content-selected optional lane inventory.
            # Unexpanded skipped matrices have no matrix values in their display names.
            $skippedMatrix = $job.name -cin 'Analyze ()', 'Test ()' -and $job.conclusion -ceq 'skipped'
            if (($job.name -cnotin $optionalCiJobs -and -not $skippedMatrix) -or $job.conclusion -cnotin 'success', 'skipped') {
                throw 'Unrecognized or unsuccessful CI job inventory.'
            }
        } elseif ($job.name -cnotin $required -or $job.conclusion -cne 'success') {
            throw "Invalid $Kind job inventory: '$($job.name)' reported '$($job.conclusion)'."
        }
    }
    foreach ($name in $required) { if (-not $names.Contains($name)) { throw "Incomplete $Kind job inventory: missing '$name'." } }
}

function Assert-ReleaseReceipt($Run, [string] $Kind) {
    $artifacts = @(Get-ReleasePages "$api/actions/runs/$($Run.id)/artifacts?per_page=100" 'artifacts')
    $matching = @($artifacts | Where-Object name -CEQ "release-evidence-$Kind-$Sha")
    if ($matching.Count -ne 1 -or $matching[0].expired -ne $false -or $matching[0].workflow_run.id -ne $Run.id -or $matching[0].id -le 0 -or
        $matching[0].size_in_bytes -le 0 -or $matching[0].size_in_bytes -gt 1048576) {
        throw "Missing, expired or ambiguous $Kind receipt artifact."
    }
    # Construct the API URL ourselves; never forward the token to an artifact-supplied URL.
    $directory = Join-Path ([IO.Path]::GetTempPath()) ('jdw-release-evidence-' + [Guid]::NewGuid().ToString('N'))
    [IO.Directory]::CreateDirectory($directory) | Out-Null
    try {
        $path = Join-Path $directory 'evidence.zip'
        Invoke-WebRequest -Uri "$api/actions/artifacts/$($matching[0].id)/zip" -Headers $headers -OutFile $path -TimeoutSec 60 | Out-Null
        if ([IO.FileInfo]::new($path).Length -gt 1048576) { throw "Invalid $Kind receipt archive size." }
        $archive = [IO.Compression.ZipFile]::OpenRead($path)
        try {
            if ($archive.Entries.Count -ne 1 -or $archive.Entries[0].FullName -cne 'receipt.json' -or $archive.Entries[0].Length -gt 16384) {
                throw "Invalid $Kind receipt archive."
            }
            $payload = $archive.Entries[0].Open()
            try {
                $bytes = [byte[]]::new(16385)
                $length = 0
                while ($length -lt $bytes.Length) {
                    $read = $payload.Read($bytes, $length, $bytes.Length - $length)
                    if ($read -eq 0) { break }
                    $length += $read
                }
                if ($length -gt 16384 -or $length -ne $archive.Entries[0].Length) { throw "Invalid $Kind receipt archive payload size." }
                try { $receipt = [Text.UTF8Encoding]::new($false, $true).GetString($bytes, 0, $length) | ConvertFrom-Json }
                catch { throw "Invalid $Kind receipt archive content." }
            } finally { $payload.Dispose() }
        } finally { $archive.Dispose() }
        $numericSchema = $receipt.schemaVersion -is [int] -or $receipt.schemaVersion -is [long]
        $numericRun = $receipt.runId -is [int] -or $receipt.runId -is [long]
        $numericAttempt = $receipt.runAttempt -is [int] -or $receipt.runAttempt -is [long]
        if (-not $numericSchema -or -not $numericRun -or -not $numericAttempt -or
            $receipt.schemaVersion -ne 1 -or $receipt.repository -ine $Repo -or
            $receipt.workflowPath -cne ".github/workflows/$Kind.yml" -or $receipt.revision -cne $Sha -or
            $receipt.runId -ne $Run.id -or $receipt.runAttempt -ne $Run.run_attempt) {
            throw "The $Kind receipt does not match its exact revision, workflow and run attempt."
        }
    } finally {
        if ([IO.File]::Exists($path)) { [IO.File]::Delete($path) }
        [IO.Directory]::Delete($directory)
    }
}

$main = Invoke-RestMethod -Uri "$api/git/ref/heads/main" -Headers $headers -TimeoutSec 60
if ($main.object.sha -cnotmatch '^[0-9a-f]{40}$') { throw 'Published main could not be resolved.' }
$comparison = Invoke-RestMethod -Uri "$api/compare/$Sha...$($main.object.sha)" -Headers $headers -TimeoutSec 60
if ($comparison.status -cnotin 'ahead', 'identical' -or $comparison.merge_base_commit.sha -cne $Sha) {
    throw "Revision $Sha is not in published main history."
}
foreach ($kind in 'ci', 'fuzz', 'portability') {
    $run = Get-ReleaseRun $kind
    Assert-ReleaseJobs $run $kind
    if ($kind -ne 'ci') { Assert-ReleaseReceipt $run $kind }
    Write-Output "Verified $kind for revision ${Sha}: https://github.com/$Repo/actions/runs/$($run.id) (attempt $($run.run_attempt))"
}
