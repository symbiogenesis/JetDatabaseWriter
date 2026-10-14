<#
.SYNOPSIS
Dispatches hosted compiler and analyzer feedback for a local patch, without publishing a branch.
.DESCRIPTION
Stage newly created files first. The patch includes committed and tracked uncommitted changes
relative to Base. GitHub limits workflow inputs to 65,535 characters, so oversized patches are
refused before dispatch. Split those into smaller candidates or use the ordinary main CI gate.
#>
[CmdletBinding()]
param([string] $Base = 'origin/main', [string] $Repo)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = Split-Path $PSScriptRoot -Parent
$baseSha = git -C $repoRoot rev-parse --verify ($Base + '^{commit}')
if ($LASTEXITCODE -ne 0 -or $baseSha -notmatch '^[0-9a-f]{40}$') { throw 'Cannot resolve the candidate base.' }
$untracked = @(git -C $repoRoot ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect untracked files.' }
if ($untracked.Count) { throw 'Stage newly created files before submitting a candidate; untracked files would be omitted.' }
$scratch = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-candidate-' + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($scratch) | Out-Null
$patchPath = Join-Path $scratch 'candidate.patch'
git -C $repoRoot diff --binary --full-index $baseSha ('--output=' + $patchPath) --
if ($LASTEXITCODE -ne 0) { throw 'Cannot create the candidate patch.' }
$bytes = [IO.File]::ReadAllBytes($patchPath)
if ($bytes.Length -eq 0) { throw 'There are no candidate changes relative to the base.' }
$inputs = @{ base_sha = $baseSha; patch_base64 = [Convert]::ToBase64String($bytes) } | ConvertTo-Json -Compress
if ([Text.Encoding]::UTF8.GetByteCount($inputs) -gt 60000) { throw "The candidate exceeds the workflow input limit. Patch retained at $patchPath." }
$ghCommand = Get-Command gh -ErrorAction SilentlyContinue
$gh = if ($ghCommand) { $ghCommand.Name } elseif ($env:ProgramFiles) { Join-Path $env:ProgramFiles 'GitHub CLI/gh.exe' } else { '' }
if (-not $gh -or (-not $ghCommand -and -not (Test-Path -LiteralPath $gh))) { throw 'GitHub CLI is required to submit a candidate.' }
$arguments = @('workflow', 'run', 'candidate.yml', '--ref', 'main', '--json')
if ($Repo) { $arguments += @('--repo', $Repo) }
Push-Location $repoRoot
try {
    $inputs | & $gh @arguments
    if ($LASTEXITCODE -ne 0) { throw "Candidate dispatch failed. Patch retained at $patchPath." }
} finally { Pop-Location }
Write-Host "Candidate dispatched on main with base $baseSha. Local patch: $patchPath"
