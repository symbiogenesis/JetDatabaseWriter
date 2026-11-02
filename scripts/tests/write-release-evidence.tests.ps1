# The success-only workflow job must bind its receipt to the checkout and GitHub identity.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$helper = Join-Path $PSScriptRoot '../write-release-evidence.ps1'
$sha = '1111111111111111111111111111111111111111'
$temporary = Join-Path ([IO.Path]::GetTempPath()) ('jdw-write-release-test-' + [Guid]::NewGuid().ToString('N'))
$names = @('GITHUB_REPOSITORY', 'JDW_VALIDATION_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_WORKFLOW_REF', 'GITHUB_REF', 'GITHUB_EVENT_NAME')
$saved = @{}
foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name) }
$context = @{ Head = $sha; ExitCode = 0 }
function git {
    if (($args -join ' ') -cne 'rev-parse --verify HEAD^{commit}') { throw 'Unexpected Git operation.' }
    $global:LASTEXITCODE = $context.ExitCode
    $context.Head
}
function Reset-Environment([string] $Kind = 'fuzz') {
    $env:GITHUB_REPOSITORY = 'example/repo'
    $env:JDW_VALIDATION_SHA = $sha
    $env:GITHUB_RUN_ID = '42'
    $env:GITHUB_RUN_ATTEMPT = '2'
    $env:GITHUB_WORKFLOW_REF = "example/repo/.github/workflows/$Kind.yml@refs/heads/main"
    $env:GITHUB_REF = 'refs/heads/main'
    $env:GITHUB_EVENT_NAME = 'workflow_run'
    $context.Head = $sha
    $context.ExitCode = 0
    if ([IO.Directory]::Exists($temporary)) {
        [IO.File]::Delete((Join-Path $temporary 'receipt.json'))
        [IO.Directory]::Delete($temporary)
    }
}
function Assert-Scenario([string] $Name, [string] $ExpectedError = '', [string] $Kind = 'fuzz') {
    $path = Join-Path $temporary 'receipt.json'
    $caught = $null
    try { & $helper -Kind $Kind -OutputPath $path | Out-Null } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or -not $caught.Contains($ExpectedError)) { throw "$Name expected '$ExpectedError', got '$caught'." }
        if ([IO.File]::Exists($path)) { throw "$Name wrote rejected evidence." }
    } elseif ($caught) { throw "$Name failed: $caught" }
    else {
        $receipt = [IO.File]::ReadAllText($path) | ConvertFrom-Json
        if ($receipt.schemaVersion -ne 1 -or $receipt.revision -cne $sha -or $receipt.repository -cne 'example/repo' -or
            $receipt.workflowPath -cne ".github/workflows/$Kind.yml" -or $receipt.runId -ne 42 -or $receipt.runAttempt -ne 2) { throw "$Name wrote incomplete evidence." }
    }
    "PASS: $Name"
}
try {
    foreach ($kind in 'fuzz', 'portability') {
        Reset-Environment $kind
        Assert-Scenario "$kind checkout produces a bound receipt" -Kind $kind
    }
    Reset-Environment; $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
    Assert-Scenario 'manual campaign produces a bound receipt'
    Reset-Environment; $context.Head = '2222222222222222222222222222222222222222'
    Assert-Scenario 'cross-SHA checkout is refused' 'checkout'
    Reset-Environment; $context.ExitCode = 128
    Assert-Scenario 'unresolved checkout is refused' 'checkout'
    foreach ($case in @(
        @{ Name = 'JDW_VALIDATION_SHA'; Value = 'short'; Error = 'revision' }
        @{ Name = 'GITHUB_REPOSITORY'; Value = 'example/repo/extra'; Error = 'repository' }
        @{ Name = 'GITHUB_RUN_ID'; Value = '0'; Error = 'run identity' }
        @{ Name = 'GITHUB_RUN_ID'; Value = '1.5'; Error = 'run identity' }
        @{ Name = 'GITHUB_RUN_ATTEMPT'; Value = '-1'; Error = 'run identity' }
        @{ Name = 'GITHUB_WORKFLOW_REF'; Value = 'fork/repo/.github/workflows/fuzz.yml@refs/heads/main'; Error = 'workflow identity' }
        @{ Name = 'GITHUB_WORKFLOW_REF'; Value = 'example/repo/.github/workflows/portability.yml@refs/heads/main'; Error = 'workflow identity' }
        @{ Name = 'GITHUB_REF'; Value = 'refs/tags/v4.0.0'; Error = 'main campaign' }
        @{ Name = 'GITHUB_EVENT_NAME'; Value = 'pull_request'; Error = 'main campaign' }
    )) {
        Reset-Environment
        [Environment]::SetEnvironmentVariable($case.Name, $case.Value)
        Assert-Scenario "invalid $($case.Name) is refused" $case.Error
    }
}
finally {
    foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $saved[$name]) }
    if ([IO.Directory]::Exists($temporary)) {
        [IO.File]::Delete((Join-Path $temporary 'receipt.json'))
        [IO.Directory]::Delete($temporary)
    }
}
