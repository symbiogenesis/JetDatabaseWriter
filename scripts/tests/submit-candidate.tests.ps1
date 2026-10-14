# Exercise dispatch input construction without publishing refs or calling GitHub.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$subject = Join-Path $PSScriptRoot '../submit-candidate.ps1'
$dispatchContext = [pscustomobject]@{ Patch = "diff --git a/old b/new`n"; Untracked = $false; Calls = [Collections.Generic.List[object]]::new() }

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function git {
    $arguments = [string[]]$args
    $global:LASTEXITCODE = 0
    if ($arguments -contains 'rev-parse') { 'a' * 40 }
    elseif ($arguments -contains 'ls-files') { if ($dispatchContext.Untracked) { 'new-file.cs' } }
    elseif ($arguments -contains 'diff') {
        $path = @($arguments | Where-Object { $_.StartsWith('--output=', [StringComparison]::Ordinal) })[0].Substring(9)
        [IO.File]::WriteAllText($path, $dispatchContext.Patch, [Text.UTF8Encoding]::new($false))
    } else { throw 'Unexpected Git command.' }
}

function gh {
    $arguments = [string[]]$args
    $body = @($input) -join "`n"
    $dispatchContext.Calls.Add([pscustomobject]@{ Arguments = $arguments; Body = $body })
    $global:LASTEXITCODE = 0
}

& $subject -Repo 'owner/repo'
Assert-Check ($dispatchContext.Calls.Count -eq 1) 'One candidate must produce exactly one dispatch.'
$call = $dispatchContext.Calls[0]
Assert-Check (($call.Arguments -join ' ') -eq 'workflow run candidate.yml --ref main --json --repo owner/repo') 'Candidates must dispatch only the main workflow.'
$body = $call.Body | ConvertFrom-Json
Assert-Check ($body.base_sha -eq ('a' * 40)) 'Dispatch must include an immutable base SHA.'
Assert-Check ([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($body.patch_base64)) -eq $dispatchContext.Patch) 'Patch bytes must survive JSON and base64 encoding.'

foreach ($scenario in 'empty', 'oversized', 'untracked') {
    $dispatchContext.Calls.Clear()
    $dispatchContext.Untracked = $scenario -eq 'untracked'
    $dispatchContext.Patch = if ($scenario -eq 'empty') { '' } elseif ($scenario -eq 'oversized') { 'x' * 50000 } else { 'patch' }
    $caught = $null
    try { & $subject } catch { $caught = $_.Exception.Message }
    Assert-Check ([bool]$caught) "$scenario candidate was not refused."
    Assert-Check ($dispatchContext.Calls.Count -eq 0) "$scenario candidate dispatched anyway."
}

$workflow = [IO.File]::ReadAllText((Join-Path $PSScriptRoot '../../.github/workflows/candidate.yml'))
Assert-Check ($workflow.Contains("github.ref == 'refs/heads/main'")) 'Candidate runners must accept main dispatch only.'
Assert-Check ($workflow.Contains('contents: read') -and $workflow.Contains('persist-credentials: false')) 'Candidate checks must not retain repository write credentials.'
Assert-Check ($workflow.Contains('CANDIDATE_PATCH: ${{ inputs.patch_base64 }}') -and -not $workflow.Contains('-EncodedPatch ${{')) 'Patch input must pass as data through an environment variable.'
Assert-Check ($workflow.Contains('name: candidate-results') -and $workflow.Contains('if: ${{ !cancelled() }}')) 'Failed candidates must retain diagnostic artifacts.'
Write-Host 'All candidate dispatch checks passed.'
