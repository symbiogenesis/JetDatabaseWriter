<#
.SYNOPSIS
Requires completed, successful CI for an exact published main revision before specialized hosted work starts.
.DESCRIPTION
Checks GitHub's existing ci.yml runs without building, dispatching, rerunning, or polling.
The newest matching push or manual main run must have passed. Its aggregate success retains
every required analyzer, test, package, and consumer gate in ci.yml. A pending or failed run
is refused even when an older run of the same revision passed. Authentication uses GITHUB_TOKEN.
#>
[CmdletBinding()]
param(
    [string] $Sha = $env:GITHUB_SHA,
    [string] $Repo = $env:GITHUB_REPOSITORY
)

$ErrorActionPreference = 'Stop'
if ($Sha -notmatch '^[0-9a-fA-F]{40}$') { throw 'Sha must be a full 40-character commit ID.' }
if ($Repo -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') { throw 'Repo must be owner/name.' }
if ([string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) { throw 'GITHUB_TOKEN is required to read the CI verdict.' }
$headers = @{
    Authorization = "Bearer $env:GITHUB_TOKEN"
    Accept = 'application/vnd.github+json'
    'User-Agent' = 'jdw-ci-prerequisite'
    'X-GitHub-Api-Version' = '2026-03-10'
}
$uri = "https://api.github.com/repos/$Repo/actions/workflows/ci.yml/runs?branch=main&head_sha=$Sha&per_page=100"
$matching = [Collections.Generic.List[object]]::new()
for ($page = 1; ; $page++) {
    $response = Invoke-RestMethod -Uri "$uri&page=$page" -Headers $headers
    $runs = @($response.workflow_runs)
    foreach ($run in $runs) {
        if ($run.head_sha -eq $Sha -and $run.head_branch -eq 'main' -and
            ($run.path -split '@', 2)[0] -eq '.github/workflows/ci.yml' -and
            $run.event -in 'push', 'workflow_dispatch') {
            $matching.Add($run)
        }
    }
    if ($runs.Count -lt 100 -or $page * 100 -ge $response.total_count) { break }
}
$latest = $matching | Sort-Object { [long]$_.id } -Descending | Select-Object -First 1
if (-not $latest) {
    throw "No CI run for main revision $Sha. Complete ci.yml for that revision before requesting specialized validation."
}
if ($latest.status -ne 'completed' -or $latest.conclusion -ne 'success') {
    throw "CI for main revision $Sha has not passed: status '$($latest.status)', conclusion '$($latest.conclusion)'. $($latest.html_url)"
}
Write-Output "Verified CI for main revision ${Sha}: $($latest.html_url)"
