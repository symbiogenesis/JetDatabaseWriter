# Guard native system configuration changes to manually dispatched disposable runners.
[CmdletBinding()]
param([string] $Repository = (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)))
$ErrorActionPreference = 'Stop'
$workflow = [IO.File]::ReadAllText((Join-Path $repository '.github/workflows/calculated-native-profiles.yml')).Replace("`r`n", "`n")
$probe = [IO.File]::ReadAllText((Join-Path $repository 'scripts/probe-calculated-native-profile.ps1'))
$installer = [IO.File]::ReadAllText((Join-Path $repository 'scripts/install-native-access.ps1'))

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Workflow([string] $Workflow, [string] $Installer) {
    Assert-Contract ($Workflow.Contains("on:`n  workflow_dispatch:`n`npermissions:")) 'Native profile probes must require manual dispatch.'
    Assert-Contract ($Workflow.Contains('runs-on: windows-latest') -and $Workflow.Contains('needs: prerequisite') -and $Workflow.Contains('./scripts/assert-ci-success.ps1')) 'Native profile probes require disposable hosted runners and completed exact-main CI.'
    Assert-Contract ($Workflow.Contains('run: ./scripts/install-native-access.ps1')) 'Native profile probes must use the shared pinned Access installer.'
    Assert-Contract ($Installer.Contains('Get-AuthenticodeSignature -LiteralPath $installer') -and $Installer.Contains("`$signature.Status -ne 'Valid'") -and $Installer.Contains("`$signature.SignerCertificate.Subject -notlike 'CN=Microsoft Corporation,*'") -and $Installer.Contains('AF1594121AB610CD8ABC3DD6FF0A504E0CBC6CE54372E5C3813926352D1179C6')) 'The deployment tool requires its reviewed digest and valid Microsoft signature.'
    Assert-Contract ($Installer.Contains('Version="16.0.20527.20112"') -and $Installer.Contains('AccessRuntimeRetail')) 'Native engine version and Runtime product must be explicit.'
    Assert-Contract ($Workflow.Contains('Set-Culture -CultureInfo $env:NATIVE_CULTURE') -and $Workflow.Contains('culture: [en-US, fr-FR, de-DE, tr-TR, ja-JP, ar-SA]')) 'Native probes must vary actual user regional profiles.'
    Assert-Contract ($Workflow.Contains('name: calculated-native-${{ matrix.culture }}') -and $Workflow.Contains('if-no-files-found: error') -and $Workflow.Contains('if: ${{ !cancelled() }}')) 'Every native profile must retain independent databases and observations.'
    Assert-Contract ($Workflow -notmatch 'continue-on-error:|runs-on: self-hosted') 'Native profile failures must remain failures on disposable hosts.'
}

function Assert-Refusal([string] $Name, [string] $Candidate, [string] $Expected, [string] $CandidateInstaller = $installer) {
    $caught = $null
    try { Assert-Workflow $Candidate $CandidateInstaller } catch { $caught = $_.Exception.Message }
    Assert-Contract ($caught -and $caught.Contains($Expected)) "$Name did not refuse for the expected reason: $caught"
}

Assert-Workflow $workflow $installer
Assert-Refusal 'automatic dispatch' ($workflow.Replace("on:`n", "on:`n  push:`n")) 'manual dispatch'
Assert-Refusal 'missing CI prerequisite' ($workflow.Replace('needs: prerequisite', 'needs: removed')) 'completed exact-main CI'
Assert-Refusal 'missing shared installer' ($workflow.Replace('run: ./scripts/install-native-access.ps1', 'run: ./scripts/removed.ps1')) 'shared pinned Access installer'
Assert-Refusal 'missing signature verification' $workflow 'reviewed digest' ($installer.Replace('Get-AuthenticodeSignature -LiteralPath $installer', 'Get-RemovedSignature -LiteralPath $installer'))
Assert-Refusal 'missing digest verification' $workflow 'reviewed digest' ($installer.Replace('AF1594121AB610CD8ABC3DD6FF0A504E0CBC6CE54372E5C3813926352D1179C6', 'removed'))
Assert-Refusal 'missing Microsoft signer' $workflow 'reviewed digest' ($installer.Replace('CN=Microsoft Corporation,*', 'CN=Removed,*'))
Assert-Refusal 'missing native version' $workflow 'Native engine version' ($installer.Replace('Version="16.0.20527.20112"', 'Version="removed"'))
Assert-Refusal 'missing native artifacts' ($workflow.Replace('if-no-files-found: error', 'if-no-files-found: warn')) 'retain independent'
Assert-Contract ($probe.Contains('The fresh process did not acquire the requested user regional profile.') -and $probe -notmatch '\bSet-Culture\b|\bSet-ItemProperty\b') 'The reusable native probe must verify profiles without changing user settings.'
Assert-Contract ($probe.Contains('$engine.CreateDatabase') -and $probe.Contains('$engine.CompactDatabase') -and $probe -notmatch '\bAccessWriter\b') 'Profile controls must be independently authored and compacted by Microsoft.'
Assert-Contract ($probe.Contains('ExtendedTables =') -and $probe.Contains('MonthNames') -and $probe.Contains('WeekNames') -and $probe.Contains('RegionalNumbers') -and $probe.Contains('RegionalDates')) 'Regional qualification requires full native name, date and numeric tables beyond the initial controls.'
Write-Output 'Native profile workflow contracts passed.'
