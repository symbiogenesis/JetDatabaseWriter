# Guard native system configuration changes to manually dispatched disposable runners.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$workflow = [IO.File]::ReadAllText((Join-Path $repository '.github/workflows/calculated-native-profiles.yml')).Replace("`r`n", "`n")
$probe = [IO.File]::ReadAllText((Join-Path $repository 'scripts/probe-calculated-native-profile.ps1'))

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Workflow([string] $Workflow) {
    Assert-Contract ($Workflow.Contains("on:`n  workflow_dispatch:`n`npermissions:")) 'Native profile probes must require manual dispatch.'
    Assert-Contract ($Workflow.Contains('runs-on: windows-latest') -and $Workflow.Contains('needs: prerequisite') -and $Workflow.Contains('./scripts/assert-ci-success.ps1')) 'Native profile probes require disposable hosted runners and completed exact-main CI.'
    Assert-Contract ($Workflow.Contains('Get-AuthenticodeSignature $installer') -and $Workflow.Contains("`$signature.Status -ne 'Valid'") -and $Workflow.Contains('AF1594121AB610CD8ABC3DD6FF0A504E0CBC6CE54372E5C3813926352D1179C6')) 'The deployment tool requires its reviewed digest and valid Microsoft signature.'
    Assert-Contract ($Workflow.Contains('Version="16.0.20527.20112"') -and $Workflow.Contains('AccessRuntimeRetail')) 'Native engine version and Runtime product must be explicit.'
    Assert-Contract ($Workflow.Contains('Set-Culture -CultureInfo $env:NATIVE_CULTURE') -and $Workflow.Contains('culture: [en-US, fr-FR, de-DE, tr-TR, ja-JP, ar-SA]')) 'Native probes must vary actual user regional profiles.'
    Assert-Contract ($Workflow.Contains('name: calculated-native-${{ matrix.culture }}') -and $Workflow.Contains('if-no-files-found: error') -and $Workflow.Contains('if: ${{ !cancelled() }}')) 'Every native profile must retain independent databases and observations.'
    Assert-Contract ($Workflow -notmatch 'continue-on-error:|runs-on: self-hosted') 'Native profile failures must remain failures on disposable hosts.'
}

function Assert-Refusal([string] $Name, [string] $Candidate, [string] $Expected) {
    $caught = $null
    try { Assert-Workflow $Candidate } catch { $caught = $_.Exception.Message }
    Assert-Contract ($caught -and $caught.Contains($Expected)) "$Name did not refuse for the expected reason: $caught"
}

Assert-Workflow $workflow
Assert-Refusal 'automatic dispatch' ($workflow.Replace("on:`n", "on:`n  push:`n")) 'manual dispatch'
Assert-Refusal 'missing CI prerequisite' ($workflow.Replace('needs: prerequisite', 'needs: removed')) 'completed exact-main CI'
Assert-Refusal 'missing signature verification' ($workflow.Replace('Get-AuthenticodeSignature $installer', 'Get-RemovedSignature $installer')) 'reviewed digest'
Assert-Refusal 'missing native artifacts' ($workflow.Replace('if-no-files-found: error', 'if-no-files-found: warn')) 'retain independent'
Assert-Contract ($probe.Contains('The fresh process did not acquire the requested user regional profile.') -and $probe -notmatch '\bSet-Culture\b|\bSet-ItemProperty\b') 'The reusable native probe must verify profiles without changing user settings.'
Assert-Contract ($probe.Contains('$engine.CreateDatabase') -and $probe.Contains('$engine.CompactDatabase') -and $probe -notmatch '\bAccessWriter\b') 'Profile controls must be independently authored and compacted by Microsoft.'
Write-Output 'Native profile workflow contracts passed.'
