<#
.SYNOPSIS
Resolves a benchmark baseline and requires published main ancestry and successful exact CI.
.DESCRIPTION
Returns the immutable baseline SHA after read-only validation, without building, dispatching or polling.
The caller must verify successful main CI for HeadSha before using this prerequisite.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Baseline,
    [string] $HeadSha = $env:GITHUB_SHA,
    [string] $Repo = $env:GITHUB_REPOSITORY,
    [string] $RepositoryRoot = (Split-Path $PSScriptRoot -Parent)
)

$ErrorActionPreference = 'Stop'
if ($HeadSha -notmatch '^[0-9a-fA-F]{40}$') { throw 'HeadSha must be a full 40-character commit ID.' }
if ($Repo -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') { throw 'Repo must be owner/name.' }
if ([string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) { throw 'GITHUB_TOKEN is required to validate the baseline.' }
$baselineSha = git -C $RepositoryRoot rev-parse --verify --end-of-options "$Baseline^{commit}" 2>$null
if ($LASTEXITCODE -ne 0 -or $baselineSha -notmatch '^[0-9a-fA-F]{40}$') { throw "Cannot resolve the baseline '$Baseline'." }
$headers = @{
    Authorization = "Bearer $env:GITHUB_TOKEN"
    Accept = 'application/vnd.github+json'
    'User-Agent' = 'jdw-benchmark-prerequisite'
    'X-GitHub-Api-Version' = '2026-03-10'
}
$comparison = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/compare/$baselineSha...$HeadSha" -Headers $headers
if ($comparison.status -notin 'ahead', 'identical') { throw 'The baseline must already belong to published main history.' }
$verdict = & "$PSScriptRoot/assert-ci-success.ps1" -Sha $baselineSha -Repo $Repo
Write-Host ($verdict -join [Environment]::NewLine)
Write-Output $baselineSha
