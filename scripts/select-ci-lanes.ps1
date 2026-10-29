# Select expensive lanes against a validated ancestor, never an unvalidated push tip.
[CmdletBinding()]
param([switch] $CheckResults)
$ErrorActionPreference = 'Stop'

function Get-CiLanes([string[]] $Paths) {
    $dotnet = $false
    $formal = $false
    foreach ($path in $Paths) {
        if ($path -cmatch '^verification/' -or
            $path -cmatch '^scripts/(?:install-dafny|verify-proofs|check-proof-review)\.ps1$' -or
            $path -cmatch '^scripts/tests/(?:verify-proofs|proof-review|formal-workflow)\.tests\.ps1$' -or
            $path -cmatch '^docs/design/(?:formal-verification|proof-trust-review|recovery-storage-contract|verification-[^/]+)\.md$') {
            $formal = $true
        } elseif ($path -cmatch '^src/JetDatabaseWriter/') {
            # The unconditional review job checks inventory and mapped C# contracts.
            # Generated code also needs the independent Dafny freshness comparison.
            $dotnet = $true
            if ($path -cmatch '^src/JetDatabaseWriter/Verification/') { $formal = $true }
        } elseif ($path -cmatch '^(?:src/|tests/|tools/|benchmarks/|Databases/)') {
            $dotnet = $true
        } elseif ($path -cmatch '^(?:docs/[^"\\]+|[^/"\\]+\.md|LICENSE(?:\.txt)?)$') {
            # Documentation still receives the unconditional review/traceability checks.
        } else {
            # Build configuration, workflow/tooling changes and unknown paths fail closed.
            $dotnet = $true
            $formal = $true
        }
    }
    return [ordered]@{ dotnet = $dotnet; formal = $formal }
}

function Get-CiBaseline([string] $Event, [string] $Ref, [string] $Sha, [string] $Repository, [string] $Token, [bool] $Full = $false) {
    # Manual and release calls always validate everything, including package production.
    if ($Full -or $Event -notin 'push', 'pull_request' -or ($Event -eq 'push' -and $Ref -ne 'refs/heads/main')) { return $null }
    if ($Sha -cnotmatch '^[0-9a-f]{40}$' -or $Repository -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$' -or -not $Token) { return $null }
    try {
        $headers = @{ Authorization = "Bearer $Token"; Accept = 'application/vnd.github+json'; 'User-Agent' = 'jdw-ci-selection'; 'X-GitHub-Api-Version' = '2026-03-10' }
        # A bounded search may do extra work; it can never justify skipping a lane.
        $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repository/actions/workflows/ci.yml/runs?branch=main&per_page=100" -Headers $headers
        $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($run in @($response.workflow_runs | Sort-Object { [long]$_.id } -Descending)) {
            if ($run.head_branch -ne 'main' -or $run.event -notin 'push', 'workflow_dispatch' -or
                ($run.path -split '@', 2)[0] -ne '.github/workflows/ci.yml' -or
                $run.head_sha -cnotmatch '^[0-9a-f]{40}$' -or $run.head_sha -eq $Sha -or -not $seen.Add($run.head_sha)) { continue }
            # A newer failed/pending rerun supersedes an older success for that SHA.
            if ($run.status -ne 'completed' -or $run.conclusion -ne 'success') { continue }
            git merge-base --is-ancestor $run.head_sha $Sha 2>$null
            if ($LASTEXITCODE -eq 0) { return $run.head_sha }
            if ($LASTEXITCODE -ne 1) { return $null }
        }
    } catch {
        Write-Warning 'Could not establish a successful CI ancestor; running every lane.'
    }
    return $null
}

function Assert-CiResults($Lanes, $Results) {
    foreach ($job in 'changes', 'checks', 'formal', 'analyze', 'aot', 'encryption-replacement') {
        $required = $job -in 'changes', 'checks' -or ($job -eq 'formal' -and $Lanes.formal) -or ($job -in 'analyze', 'aot', 'encryption-replacement' -and $Lanes.dotnet)
        $expected = if ($required) { 'success' } else { 'skipped' }
        if ($Results[$job] -ne $expected) { throw "CI job '$job' reported '$($Results[$job])'; required '$expected'." }
    }
}

if ($CheckResults) {
    $needs = $env:JDW_CI_NEEDS | ConvertFrom-Json -AsHashtable
    if ($needs.changes.outputs.dotnet -notin 'true', 'false' -or $needs.changes.outputs.formal -notin 'true', 'false') { throw 'Missing CI lane selection.' }
    $lanes = @{ dotnet = $needs.changes.outputs.dotnet -eq 'true'; formal = $needs.changes.outputs.formal -eq 'true' }
    $results = @{}
    foreach ($job in $needs.Keys) { $results[$job] = $needs[$job].result }
    Assert-CiResults $lanes $results
    Write-Output 'Every selected CI lane passed; unchanged lanes inherit the recorded successful ancestor.'
    return
}

$baseline = Get-CiBaseline $env:GITHUB_EVENT_NAME $env:GITHUB_REF $env:GITHUB_SHA $env:GITHUB_REPOSITORY $env:GITHUB_TOKEN ($env:JDW_CI_FORCE_FULL -eq 'true')
$paths = @()
if ($baseline) {
    # No API file-count cap, and --no-renames includes both sides of every rename/deletion.
    # Git quotes unusual filenames; those fail the allowlist and run every lane.
    $paths = @(git -c core.quotepath=true diff --no-renames --name-only $baseline $env:GITHUB_SHA)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot compare the validated CI ancestor with this revision.' }
    $lanes = Get-CiLanes $paths
} else {
    $lanes = [ordered]@{ dotnet = $true; formal = $true }
}
$selection = [ordered]@{ revision = $env:GITHUB_SHA; baseline = $baseline; dotnet = $lanes.dotnet; formal = $lanes.formal; paths = $paths }
$json = $selection | ConvertTo-Json -Depth 4
Write-Output $json
if ($env:GITHUB_OUTPUT) {
    foreach ($lane in 'dotnet', 'formal') { "$lane=$($lanes[$lane].ToString().ToLowerInvariant())" | Add-Content -LiteralPath $env:GITHUB_OUTPUT -Encoding utf8NoBOM }
}
if ($env:GITHUB_STEP_SUMMARY) {
    $summary = "### CI lane selection`n`nRevision: ``$($env:GITHUB_SHA)```n`nValidated ancestor: ``$baseline```n`n.NET: $($lanes.dotnet); Dafny: $($lanes.formal). Review, script and security checks always run.`n"
    Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value $summary -Encoding utf8NoBOM
}
if ($env:RUNNER_TEMP) { [IO.File]::WriteAllText((Join-Path $env:RUNNER_TEMP 'ci-selection.json'), $json) }
