# Called only by the final success-only campaign job; the publisher independently checks every job.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('fuzz', 'portability')][string] $Kind,
    [string] $OutputPath = 'artifacts/release-evidence/receipt.json'
)
$ErrorActionPreference = 'Stop'
$sha = $env:JDW_VALIDATION_SHA
$repo = $env:GITHUB_REPOSITORY
if ($sha -cnotmatch '^[0-9a-f]{40}$') { throw 'A full exact validation revision is required.' }
if ($repo -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') { throw 'A GitHub repository identity is required.' }
if ($env:GITHUB_REF -cne 'refs/heads/main' -or $env:GITHUB_EVENT_NAME -cnotin 'workflow_run', 'workflow_dispatch') {
    throw 'Release evidence requires an official main campaign.'
}
$workflowPath = ".github/workflows/$Kind.yml"
if ($env:GITHUB_WORKFLOW_REF -cne "$repo/$workflowPath@refs/heads/main") { throw 'Unexpected campaign workflow identity.' }
[long]$runId = 0
[int]$runAttempt = 0
if ($env:GITHUB_RUN_ID -notmatch '^[1-9][0-9]*$' -or -not [long]::TryParse($env:GITHUB_RUN_ID, [ref]$runId) -or
    $env:GITHUB_RUN_ATTEMPT -notmatch '^[1-9][0-9]*$' -or -not [int]::TryParse($env:GITHUB_RUN_ATTEMPT, [ref]$runAttempt)) {
    throw 'A positive numeric workflow run identity and attempt are required.'
}
$head = git rev-parse --verify 'HEAD^{commit}'
if ($LASTEXITCODE -ne 0 -or $head -cne $sha) { throw 'The actual Git checkout does not match the validation revision.' }
$receipt = [ordered]@{
    schemaVersion = 1
    repository = $repo
    workflowPath = $workflowPath
    revision = $sha
    runId = $runId
    runAttempt = $runAttempt
}
$path = [IO.Path]::GetFullPath($OutputPath)
[IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($path)) | Out-Null
$json = ($receipt | ConvertTo-Json) + "`n"
[IO.File]::WriteAllText($path, [regex]::Replace($json, "(?<!`r)`n", "`r`n"), [Text.UTF8Encoding]::new($false))
Write-Output "Recorded $Kind evidence for revision $sha, run $runId attempt $runAttempt."
