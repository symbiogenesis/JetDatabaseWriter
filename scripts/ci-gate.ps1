<#
.SYNOPSIS
Gates one commit on GitHub CI (.github/workflows/ci.yml) instead of building and testing it locally.

.DESCRIPTION
ci.yml runs on push only for main, so this script pushes the commit to a temporary ci/* branch on origin, starts
ci.yml there through workflow_dispatch, and waits for the run. Each call waits at most -WaitSeconds (default 480), so
a call stays under a 10-minute tool timeout: exit code 2 means "still running, call again with the same arguments".
Calling again never pushes or dispatches twice for the same commit on the same branch.

When the run has finished, the script downloads its logs and prints every job and step result, the build summary,
the test summaries of both legs, and the failing tests with their messages.

The script uses the GitHub CLI (gh) when it is installed and logged in, and otherwise a github.com credential stored
in git. It never prints the token. Its state and downloaded logs live in <git common dir>/ci-gate, which every
worktree shares and git never commits.

Exit codes: 0 = the run succeeded, 1 = the run failed or was cancelled, 2 = still pending (call again),
3 = usage, git or API error.

.EXAMPLE
pwsh -NoProfile -File scripts/ci-gate.ps1 -Sha 8615e8f -Branch ci/core-split-a-3

.EXAMPLE
# Re-run only the failed jobs of the last run on this branch, to tell a flaky failure from a real one, then wait.
pwsh -NoProfile -File scripts/ci-gate.ps1 -Sha 8615e8f -Branch ci/core-split-a-3 -RerunFailed

.EXAMPLE
# Report on an existing run without pushing anything.
pwsh -NoProfile -File scripts/ci-gate.ps1 -RunId 37184182298

.EXAMPLE
# Delete the temporary branch when the gate is no longer needed.
pwsh -NoProfile -File scripts/ci-gate.ps1 -Branch ci/core-split-a-3 -Delete
#>
param(
    [string] $Sha,
    [string] $Branch,
    [long] $RunId,
    [int] $WaitSeconds = 480,
    [switch] $RerunFailed,
    [switch] $Delete,
    # owner/name of the GitHub repository; defaults to the one origin points at.
    [string] $Repo
)

$ErrorActionPreference = 'Stop'
$gitDir = $PSScriptRoot

if (-not $Repo) {
    $origin = git -C $gitDir remote get-url origin 2>$null
    if ($origin -match 'github\.com[/:]([^/]+/[^/]+?)(\.git)?/?$') { $Repo = $Matches[1] }
    else { Write-Host "ERROR: cannot tell the GitHub repository from origin ('$origin'); pass -Repo owner/name."; exit 3 }
}

$api = "https://api.github.com/repos/$Repo"
$stateDir = Join-Path (git -C $gitDir rev-parse --path-format=absolute --git-common-dir) 'ci-gate'
New-Item -ItemType Directory -Force $stateDir | Out-Null

# The GitHub CLI; a session started before it was installed may not have it on PATH.
$gh = (Get-Command gh -ErrorAction SilentlyContinue).Source
if (-not $gh -and $env:ProgramFiles -and (Test-Path "$env:ProgramFiles\GitHub CLI\gh.exe")) { $gh = "$env:ProgramFiles\GitHub CLI\gh.exe" }

function Get-Headers {
    $tok = if ($gh) { (& $gh auth token 2>$null | Select-Object -First 1) } else { $null }
    if (-not $tok) {
        $cred = "protocol=https`nhost=github.com`n`n" | git -C $gitDir -c credential.interactive=never credential fill 2>$null
        $tok = ($cred | Where-Object { $_ -like 'password=*' }) -replace '^password=', ''
    }
    if (-not $tok) { Write-Host 'ERROR: no GitHub token: run "gh auth login", or store a github.com credential in git.'; exit 3 }
    return @{ Authorization = "Bearer $tok"; 'User-Agent' = 'ci-gate'; Accept = 'application/vnd.github+json'; 'X-GitHub-Api-Version' = '2022-11-28' }
}

function ConvertTo-Utc($value) {
    if ($null -eq $value) { return $null }
    if ($value -is [datetime]) { return $value.ToUniversalTime() }
    return [datetime]::Parse([string]$value, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
}

function Save-State($state, $file) { $state | ConvertTo-Json | Set-Content -LiteralPath $file -Encoding utf8NoBOM }

function Write-RunReport($run, $headers) {
    Write-Host "RUN: $($run.html_url)"
    Write-Host "HEAD: $($run.head_sha) on $($run.head_branch) (attempt $($run.run_attempt))"
    Write-Host "STATUS: $($run.status)  CONCLUSION: $($run.conclusion)"
    $jobs = Invoke-RestMethod -Uri "$api/actions/runs/$($run.id)/jobs?filter=latest" -Headers $headers
    foreach ($j in $jobs.jobs) {
        Write-Host "JOB '$($j.name)': $($j.conclusion)"
        foreach ($s in $j.steps) { Write-Host ("  step {0,2} {1}: {2}" -f $s.number, $s.name, $s.conclusion) }
    }

    $logDir = Join-Path $stateDir "run-$($run.id)-attempt$($run.run_attempt)"
    if (-not (Test-Path $logDir)) {
        $zip = "$logDir.zip"
        Invoke-WebRequest -Uri "$api/actions/runs/$($run.id)/attempts/$($run.run_attempt)/logs" -Headers $headers -OutFile $zip
        Expand-Archive -LiteralPath $zip -DestinationPath $logDir -Force
        Remove-Item -LiteralPath $zip
    }

    # The archive holds one log per job; its steps start at '##[group]Run <command>' lines.
    $jobLogs = Get-ChildItem -LiteralPath $logDir -File -Filter *.txt
    Write-Host "LOGS: $($jobLogs.FullName -join '; ')"
    Write-Host "NOTE: 'skipped' in a test summary counts the skip-guarded DAO tests and the 3 explicit-only fuzz tests together."

    foreach ($f in $jobLogs) {
        $lines = @(Get-Content -LiteralPath $f.FullName | ForEach-Object { ($_ -replace '^\d{4}-\d\d-\d\dT[\d:.]+Z ', '') -replace '\x1B\[[0-9;]*[A-Za-z]', '' })
        $step = ''
        $after = 0
        for ($i = 0; $i -lt $lines.Count; $i++) {
            $line = $lines[$i]
            if ($line -match '^##\[group\]Run (.+)$') { $step = $Matches[1]; if ($step -match 'dotnet ') { Write-Host "---- $step" }; $after = 0; continue }
            $isBuild = $step -match 'dotnet (restore|build|pack)'
            $isTest = $step -match 'dotnet test'
            if ($after -gt 0) { Write-Host "    $line"; $after--; continue }
            if ($line -match '##\[error\]') { Write-Host "  $line"; continue }
            if ($isBuild -and $line -match '(?i)^\s*\d+ (Warning|Error)\(s\)|: (error|warning) [A-Z]{2,}\d+|Build (succeeded|FAILED)') { Write-Host "  $line"; continue }
            if ($isTest -and $line -match '^failed ') { Write-Host "  $line"; $after = 12; continue }
            if ($isTest -and $line -match '^Test run summary') { Write-Host "  $line"; $after = 5; continue }
            if ($isTest -and $line -match '^skipped ' -and $i + 1 -lt $lines.Count -and $lines[$i + 1] -notmatch 'Requires Microsoft Access|explicit test filtering') { Write-Host "  NON-DAO SKIP: $line / $($lines[$i + 1].Trim())" }
        }
    }
}

if ($RunId) {
    $headers = Get-Headers
    $run = Invoke-RestMethod -Uri "$api/actions/runs/$RunId" -Headers $headers
    Write-RunReport $run $headers
    if ($run.status -ne 'completed') { exit 2 }
    if ($run.conclusion -eq 'success') { exit 0 } else { exit 1 }
}

if (-not $Branch -or $Branch -notlike 'ci/*') { Write-Host 'ERROR: -Branch must be a ci/* branch; this script never pushes main or any other branch.'; exit 3 }
$stateFile = Join-Path $stateDir (($Branch -replace '[/\\]', '_') + '.json')

if ($Delete) {
    git -C $gitDir push origin --delete $Branch 2>&1 | ForEach-Object { Write-Host $_ }
    Remove-Item -LiteralPath $stateFile -ErrorAction SilentlyContinue
    exit 0
}

if (-not $Sha) { Write-Host 'ERROR: -Sha is required.'; exit 3 }
$full = (git -C $gitDir rev-parse --verify "$Sha^{commit}" 2>$null)
if (-not $full) { Write-Host "ERROR: $Sha is not a commit in this repository."; exit 3 }
$headers = Get-Headers
$state = if (Test-Path -LiteralPath $stateFile) { Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json } else { $null }

if (-not $state -or $state.sha -ne $full) {
    git -C $gitDir push --force origin "${full}:refs/heads/$Branch" 2>&1 | ForEach-Object { Write-Host $_ }
    if ($LASTEXITCODE -ne 0) { Write-Host 'ERROR: git push failed.'; exit 3 }
    $dispatchedAt = (Get-Date).ToUniversalTime()
    if ($gh) {
        & $gh workflow run ci.yml --repo $Repo --ref $Branch 2>&1 | ForEach-Object { Write-Host $_ }
        if ($LASTEXITCODE -ne 0) { Write-Host 'ERROR: gh workflow run failed.'; exit 3 }
    }
    else {
        Invoke-RestMethod -Method Post -Uri "$api/actions/workflows/ci.yml/dispatches" -Headers $headers -ContentType 'application/json' -Body (@{ ref = $Branch } | ConvertTo-Json) | Out-Null
    }
    $state = [pscustomobject]@{ sha = $full; branch = $Branch; dispatchedAt = $dispatchedAt.ToString('o'); runId = $null }
    Save-State $state $stateFile
    Write-Host "Pushed $full to origin/$Branch and dispatched ci.yml at $($state.dispatchedAt)."
}
elseif ($RerunFailed -and $state.runId) {
    if ($gh) { & $gh run rerun $state.runId --failed --repo $Repo 2>&1 | ForEach-Object { Write-Host $_ } }
    else { Invoke-RestMethod -Method Post -Uri "$api/actions/runs/$($state.runId)/rerun-failed-jobs" -Headers $headers | Out-Null }
    Write-Host "Re-running the failed jobs of run $($state.runId)."
    Start-Sleep -Seconds 10
}

$deadline = (Get-Date).AddSeconds($WaitSeconds)
$run = $null
while ($true) {
    if (-not $state.runId) {
        $since = (ConvertTo-Utc $state.dispatchedAt).AddMinutes(-1)
        $runs = Invoke-RestMethod -Uri "$api/actions/workflows/ci.yml/runs?branch=$([uri]::EscapeDataString($Branch))&event=workflow_dispatch&per_page=10" -Headers $headers
        $run = $runs.workflow_runs | Where-Object { $_.head_sha -eq $full -and (ConvertTo-Utc $_.created_at) -ge $since } | Sort-Object { ConvertTo-Utc $_.created_at } -Descending | Select-Object -First 1
        if ($run) { $state.runId = $run.id; Save-State $state $stateFile }
    }
    else {
        $run = Invoke-RestMethod -Uri "$api/actions/runs/$($state.runId)" -Headers $headers
    }
    if ($run -and $run.status -eq 'completed') { break }
    if ((Get-Date) -ge $deadline) { break }
    Start-Sleep -Seconds 30
}

if (-not $run) { Write-Host "PENDING: dispatched at $($state.dispatchedAt); the run has not appeared yet. Call again with the same arguments."; exit 2 }
if ($run.status -ne 'completed') { Write-Host "PENDING: $($run.html_url) is $($run.status). Call again with the same arguments."; exit 2 }
Write-RunReport $run $headers
if ($run.conclusion -eq 'success') { exit 0 } else { exit 1 }
