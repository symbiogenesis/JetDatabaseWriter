# Select expensive lanes against a validated ancestor, never an unvalidated push tip.
[CmdletBinding()]
param([switch] $CheckResults)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/ci-matrix.ps1"

function Get-CiLanes([string[]] $Paths) {
    $dotnet = $false
    $formal = $false
    $package = $false
    foreach ($path in $Paths) {
        if ($path -imatch '\.dfy$' -or $path -cmatch '^verification/' -or
            $path -cmatch '^scripts/(?:install-dafny|verify-proofs|check-proof-review|select-ci-lanes|test-scripts)\.ps1$' -or
            $path -cmatch '^scripts/tests/(?:verify-proofs|proof-review|formal-workflow|select-ci-lanes|ci-workflow|ci-tools|test-scripts)\.tests\.ps1$' -or
            $path -cmatch '^docs/design/(?:formal-verification|proof-trust-review|recovery-storage-contract|verification-[^/]+)\.md$') {
            $formal = $true
        } elseif ($path -cin 'README.md', 'THIRD-PARTY-NOTICES.md') {
            # These documents ship in NuGet, but cannot change test binaries.
            $package = $true
        } elseif ($path -cmatch '^(?:(?:docs|src|tests|tools|benchmarks|Databases|\.github)/[^"\\]+\.md|[^/"\\]+\.md|LICENSE(?:\.txt)?)$') {
            # Recognize prose before its enclosing project/fixture directory.
            # Proof/trust documents above retain their stronger obligations.
        } elseif ($path -cmatch '^scripts/(?:AotSmoke/|(?:test-aot|restore-dependencies|new-native-calculated-choose-fixtures)\.ps1$)' -or
            $path -cmatch '^scripts/tests/(?:test-aot|restore-dependencies)\.tests\.ps1$') {
            $dotnet = $true
        } elseif ($path -cmatch '^scripts/tests/[^/"\\]+\.tests\.ps1$' -or
            $path -cmatch '^scripts/(?:assert-benchmark-baseline|assert-ci-success|assert-infersharp|check-security-traceability|ci-gate|compare-benchmarks|resolve-benchmark-project|run-benchmark-comparison|run-candidate-checks|run-hosted-fuzz|submit-candidate|update-sdk)\.ps1$') {
            # These tools and their regressions run in the unconditional checks.
            # Keep standalone helpers explicit; new build inputs fail closed.
        } elseif ($path -cmatch '^src/JetDatabaseWriter/') {
            # The unconditional review job checks inventory and mapped C# contracts.
            # Generated code also needs the independent Dafny freshness comparison.
            $dotnet = $true
            if ($path -cmatch '^src/JetDatabaseWriter/Verification/') { $formal = $true }
        } elseif ($path -cmatch '^(?:src/|tests/|tools/|benchmarks/|Databases/)') {
            $dotnet = $true
        } else {
            # Build configuration, workflow/tooling changes and unknown paths fail closed.
            $dotnet = $true
            $formal = $true
        }
    }
    return [ordered]@{ dotnet = $dotnet; formal = $formal; package = $package }
}

function Get-CiLaneJobs([ValidateSet('dotnet', 'formal')][string] $Lane) {
    if ($Lane -eq 'formal') { return @('Verify recovery models (Dafny)') }
    return @(
        foreach ($entry in (Get-CiAnalyzerMatrix).include) {
            if ($entry.runner) { "Analyze and test ($($entry.name))" } else { "Analyze ($($entry.name))" }
        }
        'Trimmed and NativeAOT consumers'
        'Encryption replacement (ubuntu-latest, net10.0)', 'Encryption replacement (ubuntu-latest, net11.0)',
        'Encryption replacement (macos-latest, net10.0)', 'Encryption replacement (macos-latest, net11.0)'
    )
}

function Test-CiLaneSuccess($Run, [ValidateSet('dotnet', 'formal')][string] $Lane, [string] $Repository, $Headers) {
    if ($Run.conclusion -eq 'success') { return $true }
    if ($Run.conclusion -ne 'failure') { return $false }
    # A failed independent lane cannot invalidate completed checks for unchanged
    # inputs. Require every job in this lane, plus the common review checks.
    $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repository/actions/runs/$($Run.id)/jobs?filter=latest&per_page=100" -Headers $Headers
    $jobs = @($response.jobs)
    if ($response.total_count -ne $jobs.Count) { return $false }
    $names = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($job in $jobs) { if (-not $names.Add($job.name)) { return $false } }
    $required = @('Select CI lanes', 'Script, security and proof review checks') + @(Get-CiLaneJobs $Lane)
    foreach ($name in $required) {
        $matching = @($jobs | Where-Object name -CEQ $name)
        if ($matching.Count -ne 1 -or $matching[0].status -ne 'completed' -or $matching[0].conclusion -ne 'success') { return $false }
    }
    $otherLane = if ($Lane -eq 'dotnet') { 'formal' } else { 'dotnet' }
    # Full .NET validation already packs the exact tree. A separate documentation
    # pack may be skipped there, or fail independently of a successful proof lane.
    $allowedFailures = @('CI gate', 'Package documentation') + @(Get-CiLaneJobs $otherLane)
    # New or unclassified jobs must pass too; an expanded gate cannot silently
    # inherit a result that omits a newly introduced failing obligation.
    foreach ($job in $jobs) {
        if ($job.status -ne 'completed') { return $false }
        if ($job.name -cnotin $allowedFailures -and $job.conclusion -ne 'success') { return $false }
    }
    return $true
}

function Get-CiBaseline([string] $Event, [string] $Ref, [string] $Sha, [string] $Repository, [string] $Token, [bool] $Full = $false, [ValidateSet('dotnet', 'formal')][string] $Lane = 'dotnet') {
    # Manual and release calls always validate everything, including package production.
    if ($Full -or $Event -notin 'push', 'pull_request' -or ($Event -eq 'push' -and $Ref -ne 'refs/heads/main')) { return $null }
    if ($Sha -cnotmatch '^[0-9a-f]{40}$' -or $Repository -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$' -or -not $Token) { return $null }
    try {
        $headers = @{ Authorization = "Bearer $Token"; Accept = 'application/vnd.github+json'; 'User-Agent' = 'jdw-ci-selection'; 'X-GitHub-Api-Version' = '2026-03-10' }
        # A bounded search may do extra work; it can never justify skipping a lane.
        $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repository/actions/workflows/ci.yml/runs?branch=main&per_page=100" -Headers $headers
        $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $partialChecks = 0
        foreach ($run in @($response.workflow_runs | Sort-Object { [long]$_.id } -Descending)) {
            if ($run.head_branch -ne 'main' -or $run.event -notin 'push', 'workflow_dispatch' -or
                ($run.path -split '@', 2)[0] -ne '.github/workflows/ci.yml' -or
                $run.head_sha -cnotmatch '^[0-9a-f]{40}$' -or $run.head_sha -eq $Sha -or -not $seen.Add($run.head_sha)) { continue }
            # A newer failed/pending rerun supersedes an older success for that SHA.
            if ($run.status -ne 'completed' -or $run.conclusion -notin 'success', 'failure') { continue }
            git merge-base --is-ancestor $run.head_sha $Sha 2>$null
            if ($LASTEXITCODE -eq 0) {
                if ($run.conclusion -eq 'failure' -and ++$partialChecks -gt 5) { continue }
                if (Test-CiLaneSuccess $run $Lane $Repository $headers) {
                    return [ordered]@{
                        sha = $run.head_sha
                        runId = [long]$run.id
                        url = "https://github.com/$Repository/actions/runs/$($run.id)"
                        evidence = $(if ($run.conclusion -eq 'success') { 'workflow-success' } else { 'lane-success' })
                    }
                }
                continue
            }
            if ($LASTEXITCODE -ne 1) { return $null }
        }
    } catch {
        Write-Warning "Could not establish a validated $Lane ancestor; running that lane."
    }
    return $null
}

function Assert-CiResults($Lanes, $Results) {
    foreach ($job in 'changes', 'checks', 'formal', 'analyze', 'aot', 'encryption-replacement', 'package') {
        $required = $job -in 'changes', 'checks' -or ($job -eq 'formal' -and $Lanes.formal) -or ($job -in 'analyze', 'aot', 'encryption-replacement' -and $Lanes.dotnet) -or ($job -eq 'package' -and $Lanes.package)
        $expected = if ($required) { 'success' } else { 'skipped' }
        if ($Results[$job] -ne $expected) { throw "CI job '$job' reported '$($Results[$job])'; required '$expected'." }
    }
}

if ($CheckResults) {
    $needs = $env:JDW_CI_NEEDS | ConvertFrom-Json -AsHashtable
    foreach ($lane in 'dotnet', 'formal', 'package') {
        if ($needs.changes.outputs[$lane] -notin 'true', 'false') { throw 'Missing CI lane selection.' }
    }
    $lanes = @{ dotnet = $needs.changes.outputs.dotnet -eq 'true'; formal = $needs.changes.outputs.formal -eq 'true'; package = $needs.changes.outputs.package -eq 'true' }
    if ($lanes.dotnet -and $lanes.package) { throw 'Full .NET validation already includes package validation.' }
    $results = @{}
    foreach ($job in $needs.Keys) { $results[$job] = $needs[$job].result }
    Assert-CiResults $lanes $results
    Write-Output 'Every selected CI lane passed; unchanged lanes inherit their recorded validated ancestors.'
    return
}

$baselines = [ordered]@{}
$paths = [ordered]@{}
$lanes = [ordered]@{}
$packageInputsChanged = $false
foreach ($lane in 'dotnet', 'formal') {
    $baseline = Get-CiBaseline $env:GITHUB_EVENT_NAME $env:GITHUB_REF $env:GITHUB_SHA $env:GITHUB_REPOSITORY $env:GITHUB_TOKEN ($env:JDW_CI_FORCE_FULL -eq 'true') $lane
    $baselines[$lane] = $baseline
    $paths[$lane] = @()
    $lanes[$lane] = $true
    if ($baseline) {
        # No API file-count cap; --no-renames includes both sides of every rename/deletion.
        # Quoted unusual filenames fail the allowlist and run every lane.
        $paths[$lane] = @(git -c core.quotepath=true diff --no-renames --name-only $baseline.sha $env:GITHUB_SHA)
        if ($LASTEXITCODE -ne 0) { throw 'Cannot compare the validated CI ancestor with this revision.' }
        $classified = Get-CiLanes $paths[$lane]
        $lanes[$lane] = $classified[$lane]
        if ($lane -eq 'dotnet') { $packageInputsChanged = $classified.package }
    }
}
# Documentation packaging shares the .NET ancestor: a successful full lane has
# packed these same inputs, and missing evidence already selects the full lane.
$lanes.package = -not $lanes.dotnet -and $packageInputsChanged
$selection = [ordered]@{ revision = $env:GITHUB_SHA; baselines = $baselines; dotnet = $lanes.dotnet; formal = $lanes.formal; package = $lanes.package; paths = $paths }
$json = $selection | ConvertTo-Json -Depth 4
Write-Output $json
if ($env:GITHUB_OUTPUT) {
    foreach ($lane in 'dotnet', 'formal', 'package') { "$lane=$($lanes[$lane].ToString().ToLowerInvariant())" | Add-Content -LiteralPath $env:GITHUB_OUTPUT -Encoding utf8NoBOM }
    "matrix=$(Get-CiAnalyzerMatrix | ConvertTo-Json -Depth 4 -Compress)" | Add-Content -LiteralPath $env:GITHUB_OUTPUT -Encoding utf8NoBOM
}
if ($env:GITHUB_STEP_SUMMARY) {
    $summary = "### CI lane selection`n`nRevision: ``$($env:GITHUB_SHA)```n`n.NET/package ancestor: ``$($baselines.dotnet.sha)`` $($baselines.dotnet.url)`n`nDafny ancestor: ``$($baselines.formal.sha)`` $($baselines.formal.url)`n`n.NET: $($lanes.dotnet); Dafny: $($lanes.formal); documentation package: $($lanes.package). Review, script and security checks always run.`n"
    Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value $summary -Encoding utf8NoBOM
}
if ($env:RUNNER_TEMP) { [IO.File]::WriteAllText((Join-Path $env:RUNNER_TEMP 'ci-selection.json'), $json) }
