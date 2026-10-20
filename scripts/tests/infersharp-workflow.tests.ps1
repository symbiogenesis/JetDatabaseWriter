# Guard InferSharp's independent workflow and retained analysis evidence.
[CmdletBinding()]
param([string] $WorkflowPath)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
if (-not $WorkflowPath) { $WorkflowPath = Join-Path $repoRoot '.github/workflows/infersharp.yml' }
$workflow = [IO.File]::ReadAllText($WorkflowPath).Replace("`r`n", "`n")

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Get-CiJob([string] $Workflow, [string] $Name) {
    $pattern = '(?ms)^  ' + [regex]::Escape($Name) + ':\n(?<body>.*?)(?=^  [\w-]+:\n|\z)'
    $match = [regex]::Match($Workflow, $pattern)
    Assert-Contract $match.Success "The '$Name' workflow job is missing."
    return $match.Groups['body'].Value
}

function Assert-Workflow([string] $Workflow) {
    Assert-Contract ($Workflow.Contains("name: InferSharp`n") -and $Workflow.Contains("  push:`n    branches: [main]") -and $Workflow.Contains('  pull_request:') -and $Workflow.Contains('  workflow_dispatch:')) 'InferSharp must run independently on main pushes, pull requests and manual dispatch.'
    Assert-Contract ($Workflow.Contains('group: infersharp-${{ github.ref }}') -and $Workflow -notmatch '(?m)^    needs:|assert-ci-success|workflows/ci.yml') 'InferSharp must have its own concurrency group and no ordinary CI prerequisite.'
    # Memory-safety analysis must cover both actual library assets and retain evidence
    # before findings fail the job. Roslyn gates above remain independent.
    $infer = Get-CiJob $Workflow 'infersharp'
    Assert-Contract ($infer.Contains('runs-on: ubuntu-latest') -and $infer.Contains('tf: [netstandard2.1, net10.0]')) 'InferSharp must analyze both library frameworks on Linux.'
    Assert-Contract ($infer -notmatch 'continue-on-error:|(?m)^    if:') 'InferSharp must remain a required CI gate.'
    Assert-Contract ($infer.Contains('dotnet restore JetDatabaseWriter --locked-mode') -and $infer.Contains('dotnet build JetDatabaseWriter -c Release --no-restore -f ${{ matrix.tf }}')) 'InferSharp must build the locked Release library asset for each framework.'
    Assert-Contract ($infer.Contains('-p:DebugType=portable') -and $infer.Contains('-p:DebugSymbols=true')) 'InferSharp must receive source symbols.'
    Assert-Contract ($infer.Contains('mcr.microsoft.com/infersharp@sha256:030eb68fb99f86f6c6351672f444a7092dceb0fdf15ac5f24ddf9f9e198484cd') -and $infer.Contains('tee /workspace/artifacts/infersharp/analysis.log')) 'InferSharp must pin v1.5 and preserve translation coverage.'
    $reportsPosition = $infer.IndexOf('      - name: Upload InferSharp reports', [StringComparison]::Ordinal)
    $gatePosition = $infer.IndexOf('      - name: Require no unreviewed InferSharp findings', [StringComparison]::Ordinal)
    Assert-Contract ($reportsPosition -ge 0 -and $gatePosition -gt $reportsPosition -and $infer.Contains('if: ${{ !cancelled() }}') -and $infer.Contains('name: infersharp-${{ matrix.tf }}') -and $infer.Contains('if-no-files-found: error')) 'InferSharp must retain reports before gating findings.'
    Assert-Contract ($infer.Contains('./scripts/assert-infersharp.ps1 -ReportPath artifacts/infersharp/report.sarif -TranslationLogPath artifacts/infersharp/analysis.log -BaselinePath .github/infersharp-baseline.json -TargetFramework ${{ matrix.tf }}')) 'InferSharp must reject findings and empty translation.'
}

function Assert-Scenario([string] $Name, [string] $Workflow, [string] $ExpectedError = '') {
    $caught = $null
    try { Assert-Workflow $Workflow } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        Assert-Contract ($caught -and $caught.Contains($ExpectedError)) "$Name expected '$ExpectedError', got '$caught'."
    } else { Assert-Contract (-not $caught) "$Name failed: $caught" }
    "PASS: $Name"
}

Assert-Scenario 'independent InferSharp workflow' $workflow
Assert-Scenario 'missing framework coverage' ($workflow.Replace('tf: [netstandard2.1, net10.0]', 'tf: [net10.0]')) 'InferSharp must analyze both library frameworks'
Assert-Scenario 'analysis allowed to fail' ($workflow.Replace("  infersharp:`n", "  infersharp:`n    continue-on-error: true`n")) 'InferSharp must remain a required CI gate'
Assert-Scenario 'missing finding gate' ($workflow.Replace('./scripts/assert-infersharp.ps1', './scripts/removed-infersharp.ps1')) 'InferSharp must reject findings and empty translation'
Assert-Scenario 'missing translation log' ($workflow.Replace('tee /workspace/artifacts/infersharp/analysis.log', 'cat')) 'InferSharp must pin v1.5 and preserve translation coverage'
Assert-Scenario 'shared ordinary CI concurrency' ($workflow.Replace('group: infersharp-', 'group: ci-')) 'InferSharp must have its own concurrency group'
Assert-Scenario 'ordinary CI dependency' ($workflow.Replace("  infersharp:`n", "  infersharp:`n    needs: ordinary-ci`n")) 'InferSharp must have its own concurrency group'
Assert-Scenario 'missing automatic push' ($workflow.Replace('  push:', '  removed-push:')) 'InferSharp must run independently'
