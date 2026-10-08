<#
.SYNOPSIS
Exercises ci-gate with mocked Git and GitHub responses; never publishes or contacts GitHub.
#>
$ErrorActionPreference = 'Stop'
$helper = Join-Path $PSScriptRoot '../ci-gate.ps1'
$testRoot = Join-Path $PSScriptRoot "../../artifacts/ci-gate-tests-$([guid]::NewGuid().ToString('N'))"
[IO.Directory]::CreateDirectory($testRoot) | Out-Null
$sha = '1111111111111111111111111111111111111111'
$otherSha = '2222222222222222222222222222222222222222'

function New-Fixture([string] $name, [string] $status = 'completed', [string] $conclusion = 'success') {
    $script:fixture = @{
        root = Join-Path $testRoot $name
        calls = [Collections.Generic.List[string]]::new()
        run = [pscustomobject]@{
            id = 42; head_sha = $sha; head_branch = 'main'; run_attempt = 1; path = '.github/workflows/ci.yml@main'
            html_url = 'https://github.com/example/repo/actions/runs/42'
            status = $status; conclusion = $conclusion; created_at = [datetime]::UtcNow.ToString('o')
        }
        remoteSha = $sha
        runs = $true
        baselineStatus = 'ahead'
    }
    [IO.Directory]::CreateDirectory($fixture.root) | Out-Null
}

function git {
    $fixture.calls.Add("git $($args -join ' ')")
    $global:LASTEXITCODE = 0
    switch -Regex ($args -join ' ') {
        'remote get-url origin$' { 'https://github.com/example/repo.git'; return }
        'rev-parse --path-format=absolute --git-common-dir$' { $fixture.root; return }
        'rev-parse --verify.*2222222' { $otherSha; return }
        'rev-parse --verify' { $sha; return }
        default { throw "Unexpected Git call: $($args -join ' ')" }
    }
}

function Get-Command {
    param([string] $Name, [string] $ErrorAction)
    if ($Name -ne 'gh') { throw "Unexpected Get-Command: $Name" }
    [pscustomobject]@{ Source = 'Invoke-MockGh' }
}

function Invoke-MockGh {
    if (($args -join ' ') -ne 'auth token') { throw "Unexpected gh call: $($args -join ' ')" }
    'mock-token'
}

function Invoke-RestMethod {
    param([string] $Uri, $Headers, [string] $Method = 'Get', [string] $ContentType, [string] $Body)
    $fixture.calls.Add("$Method $Uri $Body")
    switch -Regex ($Uri) {
        '/branches/main$' { return [pscustomobject]@{ commit = @{ sha = $fixture.remoteSha } } }
        '/compare/' { return [pscustomobject]@{ status = $fixture.baselineStatus } }
        '/dispatches$' {
            if (($Body | ConvertFrom-Json).ref -ne 'main') { throw 'Dispatched a branch other than main.' }
            if ($Headers['X-GitHub-Api-Version'] -ne '2026-03-10') { throw 'Dispatch run IDs require the current API version.' }
            if ($fixture.missingDispatchId) { return }
            return [pscustomobject]@{ workflow_run_id = 42; html_url = $fixture.run.html_url }
        }
        '/rerun-failed-jobs$' {
            if ($fixture.rejectRerun) { throw 'The mocked rerun request was rejected.' }
            if (-not $fixture.staleRerun) { $fixture.run.run_attempt++; $fixture.run.status = 'queued' }
            return
        }
        '/runs/42/attempts/\d+/jobs\?' {
            return [pscustomobject]@{ total_count = 1; jobs = @(@{ name = 'Test'; conclusion = $fixture.run.conclusion; steps = @(@{ number = 1; name = 'Test'; conclusion = $fixture.run.conclusion }) }) }
        }
        '/runs/42/artifacts\??' { return [pscustomobject]@{ total_count = 0; artifacts = @() } }
        '/workflows/.+/runs\?' { return [pscustomobject]@{ workflow_runs = $(if ($fixture.runs) { @($fixture.run) } else { @() }) } }
        '/runs/42$' { return $fixture.run }
        default { throw "Unexpected API call: $Uri" }
    }
}

function Invoke-WebRequest {
    param([string] $Uri, $Headers, [string] $OutFile)
    $fixture.calls.Add("DOWNLOAD $Uri")
    if ($fixture.run.status -ne 'completed') { throw 'Pending logs must not be requested.' }
    $archiveSource = Join-Path $fixture.root 'archive-source'
    [IO.Directory]::CreateDirectory($archiveSource) | Out-Null
    [IO.File]::WriteAllText((Join-Path $archiveSource 'Test.txt'), @'
2026-10-07T00:00:00.000Z ##[group]Run dotnet build example
2026-10-07T00:00:00.000Z Build succeeded.
2026-10-07T00:00:00.000Z     0 Warning(s)
2026-10-07T00:00:00.000Z     0 Error(s)
2026-10-07T00:00:00.000Z ##[group]Run dotnet test example
2026-10-07T00:00:00.000Z skipped DAOCase
2026-10-07T00:00:00.000Z Requires Microsoft Access (DAO.DBEngine.120)
2026-10-07T00:00:00.000Z skipped OtherCase
2026-10-07T00:00:00.000Z An unexpected reason
2026-10-07T00:00:00.000Z Test run summary: Passed!
2026-10-07T00:00:00.000Z   total: 2
2026-10-07T00:00:00.000Z   failed: 0
2026-10-07T00:00:00.000Z   succeeded: 1
2026-10-07T00:00:00.000Z   skipped: 1
2026-10-07T00:00:00.000Z   duration: 1s
'@)
    Compress-Archive -LiteralPath (Join-Path $archiveSource 'Test.txt') -DestinationPath $OutFile -Force
}

function Invoke-Helper([hashtable] $arguments) {
    $script:output = (& $helper @arguments 6>&1 | Out-String)
    $script:exitCode = $LASTEXITCODE
}

function Assert([bool] $condition, [string] $message) {
    if (-not $condition) { throw "FAIL: $message`n$output" }
}

New-Fixture 'pending' 'in_progress' ''
Invoke-Helper @{ RunId = 42 }
Assert ($exitCode -eq 2) 'Pending reports must return 2 without downloading unavailable logs.'
Assert (-not ($fixture.calls -match 'DOWNLOAD|/jobs')) 'Pending reports should make only the run request.'

New-Fixture 'report'
Invoke-Helper @{ RunId = 42 }
Assert ($exitCode -eq 0) 'Successful report should return 0.'
Assert ($output -match 'Build succeeded' -and $output -match 'Test run summary') 'Build and test summaries must survive streaming.'
Assert ($output -match 'NON-DAO SKIP: skipped OtherCase' -and $output -notmatch 'NON-DAO SKIP: skipped DAOCase') 'Skip lookahead should flag only unexpected reasons.'
Invoke-Helper @{ RunId = 42 }
Assert (@($fixture.calls -match 'DOWNLOAD').Count -eq 1) 'Completed logs should be downloaded once.'
Assert (@($fixture.calls -match '/jobs').Count -eq 1) 'Completed job metadata should be fetched once per attempt.'
Assert (-not ($fixture.calls -match '/artifacts')) 'CI reports should not request benchmark artifacts.'

New-Fixture 'reuse'
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert ($exitCode -eq 0) 'An existing main run should be reused.'
Assert (-not ($fixture.calls -match 'dispatches|git .*push')) 'An existing run should not publish or dispatch.'

New-Fixture 'dispatch' 'queued' ''
$fixture.runs = $false
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert ($exitCode -eq 2) 'A new dispatch should return pending.'
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert (@($fixture.calls -match 'dispatches').Count -eq 1) 'Repeated calls must not dispatch twice.'
Assert (-not ($fixture.calls -match 'git .*push')) 'The helper must never publish branches or tags.'

New-Fixture 'benchmark-dispatch' 'queued' ''
$fixture.run.path = '.github/workflows/benchmarks.yml'
Invoke-Helper @{ Sha = $sha; Benchmarks = '*Example*'; Baseline = $otherSha; WaitSeconds = 0 }
Assert ($exitCode -eq 2) 'Benchmark dispatch should return pending for its exact run ID.'
Assert (-not ($fixture.calls -match '/workflows/.+/runs\?')) 'Benchmark runs must not be inferred from timestamps or unrelated concurrent dispatches.'

New-Fixture 'missing-dispatch-id' 'queued' ''
$fixture.runs = $false
$fixture.missingDispatchId = $true
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert ($exitCode -eq 3) 'An unconfirmed dispatch must require explicit run selection.'
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert ($exitCode -eq 3) 'An unconfirmed dispatch must not be guessed on retry.'
Assert (@($fixture.calls -match 'dispatches').Count -eq 1) 'A missing dispatch response must not cause duplicate runs.'

New-Fixture 'wrong-main'
$fixture.remoteSha = $otherSha
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert ($exitCode -eq 3) 'A SHA that is not published main must be rejected.'
Assert (-not ($fixture.calls -match 'dispatches')) 'A wrong SHA must not dispatch.'

New-Fixture 'bad-baseline'
$fixture.baselineStatus = 'diverged'
Invoke-Helper @{ Sha = $sha; Benchmarks = '*Example*'; Baseline = $otherSha; WaitSeconds = 0 }
Assert ($exitCode -eq 3) 'A baseline outside published main history must be rejected.'

New-Fixture 'failed' 'completed' 'failure'
Invoke-Helper @{ RunId = 42 }
Assert ($exitCode -eq 1) 'Failed runs must return 1.'

New-Fixture 'rerun' 'completed' 'failure'
$fixture.staleRerun = $true
Invoke-Helper @{ Sha = $sha; RerunFailed = $true; WaitSeconds = 0 }
Assert ($exitCode -eq 2) 'Rerun requests must return pending until the new attempt appears.'
Invoke-Helper @{ Sha = $sha; RerunFailed = $true; WaitSeconds = 0 }
Assert ($exitCode -eq 2) 'Stale completed attempts must not be reported as the retry result.'
$fixture.run.run_attempt = 2
Invoke-Helper @{ Sha = $sha; RerunFailed = $true; WaitSeconds = 0 }
Assert ($exitCode -eq 1) 'A failed retry must return failure instead of rerunning indefinitely.'
Assert (@($fixture.calls -match 'rerun-failed-jobs').Count -eq 1) 'Repeated arguments must retry failed jobs only once.'

New-Fixture 'unconfirmed-rerun' 'completed' 'failure'
$fixture.rejectRerun = $true
Invoke-Helper @{ Sha = $sha; RerunFailed = $true; WaitSeconds = 0 }
Assert ($exitCode -eq 3) 'A rejected or uncertain rerun request must return an error.'
Invoke-Helper @{ Sha = $sha; RerunFailed = $true; WaitSeconds = 0 }
Assert ($exitCode -eq 3 -and $output -match 'not confirmed.*-RunId') 'An unconfirmed rerun must require explicit recovery instead of remaining pending indefinitely.'
Assert (@($fixture.calls -match 'rerun-failed-jobs').Count -eq 1) 'An unconfirmed rerun must not be submitted again automatically.'

New-Fixture 'mismatched-cache' 'queued' ''
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
$fixture.run.head_sha = $otherSha
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert ($exitCode -eq 3) 'Cached runs must still match the requested SHA.'

New-Fixture 'mismatched-workflow' 'queued' ''
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
$fixture.run.path = '.github/workflows/other.yml'
Invoke-Helper @{ Sha = $sha; WaitSeconds = 0 }
Assert ($exitCode -eq 3) 'Cached runs must still match the requested workflow.'

Write-Host "PASS: ci-gate regression checks. Fixtures: $testRoot"
