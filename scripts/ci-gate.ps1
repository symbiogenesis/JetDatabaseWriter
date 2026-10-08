<#
.SYNOPSIS
Gates a published main commit on GitHub CI. Local builds and targeted tests are allowed.

.DESCRIPTION
Integrate and publish main before calling this helper. It never pushes branches or tags. For -Sha, it verifies
GitHub's main tip, reuses an existing CI run for that SHA, or dispatches ci.yml on main if none exists. Benchmark
requests dispatch benchmarks.yml on main; a baseline must already belong to published main history.

The default checks once and returns immediately. -WaitSeconds optionally bounds polling. Exit code 2 means the
run is pending; call again with the same arguments. State is shared by worktrees and prevents repeated dispatches
for the same SHA and inputs. Dispatch responses identify the exact run, including concurrent benchmark requests.
-RunId reports an existing run without dispatching anything.

Completed reports print every job and step result, build and test summaries, failing-test messages and unexpected
skip reasons. Logs are read a line at a time, and completed job metadata and downloads are cached per run attempt
under <git common dir>/ci-gate/<owner>/<repo>. The helper uses gh or Git's github.com credential without printing it.

Exit codes: 0 = succeeded, 1 = failed or cancelled, 2 = pending, 3 = usage, git or API error.

.EXAMPLE
pwsh -NoProfile -File scripts/ci-gate.ps1 -Sha 8615e8f

.EXAMPLE
pwsh -NoProfile -File scripts/ci-gate.ps1 -Sha 519bba1 -Benchmarks '*AccessWriterBenchmarks*' -Baseline 56d84f3

.EXAMPLE
# Retry only failed jobs of the matching CI run, then report its current status.
pwsh -NoProfile -File scripts/ci-gate.ps1 -Sha 8615e8f -RerunFailed

.EXAMPLE
pwsh -NoProfile -File scripts/ci-gate.ps1 -RunId 37184182298
#>
[CmdletBinding()]
param(
    [string] $Sha,
    [long] $RunId,
    [ValidateRange(0, 86400)] [int] $WaitSeconds = 0,
    [switch] $RerunFailed,
    [string] $Benchmarks,
    [string] $Baseline,
    [ValidateSet('default', 'short', 'medium', 'dry')] [string] $Job = 'default',
    [string] $Repo
)

$ErrorActionPreference = 'Stop'
$gitDir = $PSScriptRoot

function Get-Headers {
    $gh = (Get-Command gh -ErrorAction SilentlyContinue).Source
    if (-not $gh -and $env:ProgramFiles -and (Test-Path "$env:ProgramFiles\GitHub CLI\gh.exe")) {
        $gh = "$env:ProgramFiles\GitHub CLI\gh.exe"
    }
    $token = if ($gh) { & $gh auth token 2>$null | Select-Object -First 1 } else { $null }
    if (-not $token) {
        $credential = "protocol=https`nhost=github.com`n`n" | git -C $gitDir -c credential.interactive=never credential fill 2>$null
        $token = ($credential | Where-Object { $_ -like 'password=*' }) -replace '^password=', ''
    }
    if (-not $token) { throw 'No GitHub token: run "gh auth login", or store a github.com credential in git.' }
    return @{ Authorization = "Bearer $token"; 'User-Agent' = 'ci-gate'; Accept = 'application/vnd.github+json'; 'X-GitHub-Api-Version' = '2026-03-10' }
}

function Save-Json($value, [string] $file) {
    $temporary = "$file.$([guid]::NewGuid().ToString('N')).tmp"
    [IO.File]::WriteAllText($temporary, ($value | ConvertTo-Json -Depth 20))
    [IO.File]::Move($temporary, $file, $true)
}

function Get-Collection([string] $uri, [string] $property) {
    $separator = if ($uri.Contains('?')) { '&' } else { '?' }
    for ($page = 1; ; $page++) {
        $response = Invoke-RestMethod -Uri "${uri}${separator}per_page=100&page=$page" -Headers $headers
        $items = @($response.$property)
        $items
        if ($items.Count -lt 100 -or $page * 100 -ge $response.total_count) { break }
    }
}

function Get-Archive([string] $uri, [string] $directory) {
    if ([IO.Directory]::Exists($directory)) { return }
    # Publish the cache directory only after extraction succeeds, so a partial download is never reused.
    $temporary = "$directory.$([guid]::NewGuid().ToString('N'))"
    $zip = "$temporary.zip"
    Invoke-WebRequest -Uri $uri -Headers $headers -OutFile $zip
    Expand-Archive -LiteralPath $zip -DestinationPath $temporary
    try { [IO.Directory]::Move($temporary, $directory) }
    catch [IO.IOException] {
        if (-not [IO.Directory]::Exists($directory)) { throw }
        # Another reporter completed the same immutable attempt while this download was in flight.
    }
    Remove-Item -LiteralPath $zip
}

function Write-RunStatus($run) {
    Write-Host "RUN: $($run.html_url)"
    Write-Host "HEAD: $($run.head_sha) on $($run.head_branch) (attempt $($run.run_attempt))"
    Write-Host "STATUS: $($run.status)  CONCLUSION: $($run.conclusion)"
}

function Write-RunReport($run) {
    Write-RunStatus $run
    if ($run.status -ne 'completed') { return }

    $prefix = Join-Path $stateDir "run-$($run.id)-attempt$($run.run_attempt)"
    $jobsFile = "$prefix-jobs.json"
    if ([IO.File]::Exists($jobsFile)) { $jobs = (Get-Content -LiteralPath $jobsFile -Raw | ConvertFrom-Json).jobs }
    else {
        $jobs = @(Get-Collection "$api/actions/runs/$($run.id)/attempts/$($run.run_attempt)/jobs" 'jobs')
        Save-Json @{ jobs = $jobs } $jobsFile
    }
    foreach ($jobResult in $jobs) {
        Write-Host "JOB '$($jobResult.name)': $($jobResult.conclusion)"
        foreach ($stepResult in $jobResult.steps) {
            Write-Host ('  step {0,2} {1}: {2}' -f $stepResult.number, $stepResult.name, $stepResult.conclusion)
        }
    }

    Get-Archive "$api/actions/runs/$($run.id)/attempts/$($run.run_attempt)/logs" $prefix
    $jobLogs = @(Get-ChildItem -LiteralPath $prefix -File -Filter '*.txt' | Sort-Object Name)
    Write-Host "LOGS: $($jobLogs.FullName -join '; ')"
    Write-Host "NOTE: 'skipped' in a test summary counts the skip-guarded DAO tests and the 3 explicit-only fuzz tests together."
    $decoration = [regex]::new('^\d{4}-\d\d-\d\dT[\d:.]+Z |\x1B\[[0-9;]*[A-Za-z]')
    foreach ($file in $jobLogs) {
        $isBuild = $false
        $isTest = $false
        $after = 0
        $skip = $null
        foreach ($rawLine in [IO.File]::ReadLines($file.FullName)) {
            $line = $decoration.Replace($rawLine, '')
            if ($skip) {
                if ($line -notmatch 'Requires Microsoft Access|explicit test filtering') {
                    Write-Host "  NON-DAO SKIP: $skip / $($line.Trim())"
                }
                $skip = $null
            }
            if ($line -match '^##\[group\]Run (.+)$') {
                $step = $Matches[1]
                $isBuild = $step -match 'dotnet (restore|build|pack)'
                $isTest = $step -match 'dotnet test'
                if ($step -match 'dotnet ') { Write-Host "---- $step" }
                $after = 0
                continue
            }
            if ($isTest -and $line -match '^skipped ') { $skip = $line }
            if ($after -gt 0) { Write-Host "    $line"; $after--; continue }
            if ($line -match '##\[error\]') { Write-Host "  $line"; continue }
            if ($isBuild -and $line -match '(?i)^\s*\d+ (Warning|Error)\(s\)|: (error|warning) [A-Z]{2,}\d+|Build (succeeded|FAILED)') { Write-Host "  $line"; continue }
            if ($isTest -and $line -match '^failed ') { Write-Host "  $line"; $after = 12; continue }
            if ($isTest -and $line -match '^Test run summary') { Write-Host "  $line"; $after = 5 }
        }
        if ($skip) { Write-Host "  NON-DAO SKIP: $skip / missing skip reason" }
    }
}

function Write-BenchmarkSummary($run) {
    $directory = Join-Path $stateDir "run-$($run.id)-attempt$($run.run_attempt)-benchmark-results"
    if (-not [IO.Directory]::Exists($directory)) {
        $artifact = Get-Collection "$api/actions/runs/$($run.id)/artifacts" 'artifacts' |
            Where-Object { $_.name -eq 'benchmark-results' -and -not $_.expired } | Select-Object -First 1
        if (-not $artifact) { return }
        Get-Archive $artifact.archive_download_url $directory
    }
    Write-Host "BENCHMARK RESULTS: $directory"
    $summary = Get-ChildItem -LiteralPath $directory -Recurse -File -Filter 'summary.md' | Select-Object -First 1
    if ($summary) { foreach ($line in [IO.File]::ReadLines($summary.FullName)) { Write-Host $line } }
}

function Find-Run([string] $workflow, [string] $full) {
    $uri = "$api/actions/workflows/$workflow/runs?branch=main&head_sha=$full&per_page=100"
    $runs = (Invoke-RestMethod -Uri $uri -Headers $headers).workflow_runs
    return $runs | Where-Object {
        $_.head_sha -eq $full -and $_.head_branch -eq 'main'
    } | Sort-Object id -Descending | Select-Object -First 1
}

function Assert-RunRevision($run, [string] $full, [string] $workflow) {
    if ($run.head_sha -ne $full -or $run.head_branch -ne 'main' -or ($run.path -split '@', 2)[0] -ne ".github/workflows/$workflow") {
        throw 'The discovered run does not match the requested main revision and workflow.'
    }
}

try {
    if ($RunId -lt 0 -or (-not $RunId -and -not $Sha)) { throw 'Specify -Sha or -RunId.' }
    if ($RunId -and ($Sha -or $Benchmarks -or $Baseline -or $RerunFailed)) { throw '-RunId cannot be combined with dispatch or rerun arguments.' }
    if ($Baseline -and -not $Benchmarks) { throw '-Baseline needs -Benchmarks.' }
    if (-not $Repo) {
        $origin = git -C $gitDir remote get-url origin 2>$null
        if ($LASTEXITCODE -ne 0 -or $origin -notmatch 'github\.com[/:]([^/]+/[^/]+?)(\.git)?/?$') {
            throw "Cannot tell the GitHub repository from origin ('$origin'); pass -Repo owner/name."
        }
        $Repo = $Matches[1]
    }
    if ($Repo -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') { throw '-Repo must be owner/name.' }
    $api = "https://api.github.com/repos/$Repo"
    $commonDir = git -C $gitDir rev-parse --path-format=absolute --git-common-dir
    if ($LASTEXITCODE -ne 0 -or -not $commonDir) { throw 'Cannot find the Git common directory.' }
    $stateDir = Join-Path $commonDir "ci-gate/$Repo"
    [IO.Directory]::CreateDirectory($stateDir) | Out-Null
    $headers = Get-Headers

    if ($RunId) {
        $run = Invoke-RestMethod -Uri "$api/actions/runs/$RunId" -Headers $headers
    }
    else {
        $full = git -C $gitDir rev-parse --verify --end-of-options "$Sha^{commit}" 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $full) { throw "$Sha is not a commit in this repository." }
        $workflow = if ($Benchmarks) { 'benchmarks.yml' } else { 'ci.yml' }
        $inputs = [ordered]@{}
        if ($Benchmarks) {
            $inputs.filter = $Benchmarks
            $inputs.job = $Job
            $inputs.baseline = ''
            if ($Baseline) {
                $inputs.baseline = git -C $gitDir rev-parse --verify --end-of-options "$Baseline^{commit}" 2>$null
                if ($LASTEXITCODE -ne 0 -or -not $inputs.baseline) { throw "$Baseline is not a commit in this repository." }
            }
        }
        $inputsJson = $inputs | ConvertTo-Json -Compress
        $inputHash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($inputsJson))).ToLowerInvariant()
        $stateFile = Join-Path $stateDir "$workflow-$full-$inputHash.json"
        # Serialize discovery and dispatch across worktrees without holding the lock during polling or reporting.
        $stateLock = [IO.File]::Open("$stateFile.lock", [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
        try {
            $state = if ([IO.File]::Exists($stateFile)) { Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json } else { $null }
            if ($state -and $state.rerunRequested -and -not $state.rerunConfirmed) {
                throw 'The previous rerun request was not confirmed. Inspect GitHub Actions and report the intended attempt with -RunId.'
            }
            $run = $null
            if ($state -and $state.runId) { $run = Invoke-RestMethod -Uri "$api/actions/runs/$($state.runId)" -Headers $headers }
            elseif ($state) { throw 'A previous dispatch has no confirmed run ID. Inspect GitHub Actions and report the intended run with -RunId.' }
            else {
                $main = Invoke-RestMethod -Uri "$api/branches/main" -Headers $headers
                if ($main.commit.sha -ne $full) { throw "$full is not GitHub's main tip. Integrate and publish main before gating it." }
                if ($inputs.baseline) {
                    $comparison = Invoke-RestMethod -Uri "$api/compare/$($inputs.baseline)...$full" -Headers $headers
                    if ($comparison.status -notin 'ahead', 'identical') { throw 'The baseline must already belong to published main history.' }
                }
                if (-not $Benchmarks) { $run = Find-Run $workflow $full }
                if (-not $run -and $RerunFailed) { throw 'No matching run exists to rerun.' }
                $state = [pscustomobject]@{ sha = $full; workflow = $workflow; inputs = $inputsJson; dispatchedAt = $null; runId = $null; minimumAttempt = 1; rerunRequested = $false; rerunConfirmed = $false }
                if (-not $run) {
                    $state.dispatchedAt = [datetimeoffset]::UtcNow.ToString('o')
                    $body = @{ ref = 'main' }
                    if ($inputs.Count -gt 0) { $body.inputs = $inputs }
                    # Save intent first: a lost HTTP response must not cause another dispatch on retry.
                    Save-Json $state $stateFile
                    # API 2026-03-10 returns the run ID instead of requiring timestamp-based discovery:
                    # https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event
                    $dispatch = Invoke-RestMethod -Method Post -Uri "$api/actions/workflows/$workflow/dispatches" -Headers $headers -ContentType 'application/json' -Body ($body | ConvertTo-Json)
                    $state.runId = $dispatch.workflow_run_id
                    Save-Json $state $stateFile
                    if (-not $state.runId) { throw 'The dispatch response did not identify a run. Inspect GitHub Actions and use -RunId; this request will not dispatch again.' }
                    Write-Host "Dispatched $workflow on main for ${full}: $($dispatch.html_url)"
                    $run = Invoke-RestMethod -Uri "$api/actions/runs/$($state.runId)" -Headers $headers
                }
            }
            if ($run) {
                Assert-RunRevision $run $full $workflow
                $state.runId = $run.id
                if ($RerunFailed -and -not $state.rerunRequested -and $run.status -eq 'completed' -and $run.conclusion -ne 'success') {
                    $state.minimumAttempt = $run.run_attempt + 1
                    $state.rerunRequested = $true
                    Save-Json $state $stateFile
                    Invoke-RestMethod -Method Post -Uri "$api/actions/runs/$($run.id)/rerun-failed-jobs" -Headers $headers | Out-Null
                    $state.rerunConfirmed = $true
                    Save-Json $state $stateFile
                    Write-Host "Re-running failed jobs of run $($run.id); waiting for attempt $($state.minimumAttempt)."
                    $run = $null
                }
            }
            Save-Json $state $stateFile
        }
        finally { $stateLock.Dispose() }

        $timer = [Diagnostics.Stopwatch]::StartNew()
        while ($true) {
            if ($run -and $run.run_attempt -ge $state.minimumAttempt -and $run.status -eq 'completed') { break }
            if ($timer.Elapsed.TotalSeconds -ge $WaitSeconds) { break }
            Start-Sleep -Milliseconds ([int][Math]::Max(0, [Math]::Min(15000, [Math]::Ceiling(($WaitSeconds - $timer.Elapsed.TotalSeconds) * 1000))))
            $run = Invoke-RestMethod -Uri "$api/actions/runs/$($state.runId)" -Headers $headers
            if ($run) { Assert-RunRevision $run $full $workflow; $state.runId = $run.id }
        }
        if ($run -and $run.run_attempt -lt $state.minimumAttempt) { $run = $null }
    }

    if (-not $run) { Write-Host 'PENDING: the requested run or rerun has not appeared yet. Call again with the same arguments.'; exit 2 }
    Write-RunReport $run
    if ($run.status -ne 'completed') { exit 2 }
    if ($Benchmarks -or ($run.path -split '@', 2)[0] -eq '.github/workflows/benchmarks.yml') { Write-BenchmarkSummary $run }
    if ($run.conclusion -eq 'success') { exit 0 }
    exit 1
}
catch {
    Write-Host "ERROR: $($_.Exception.Message)"
    exit 3
}
