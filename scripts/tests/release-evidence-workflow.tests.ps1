# Publication must consume exact-revision campaigns and preserve their success-only receipts.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Get-Job([string] $Workflow, [string] $Name) {
    return [regex]::Match($Workflow, '(?ms)^  ' + [regex]::Escape($Name) + ':\n(?<body>.*?)(?=^  [\w-]+:\n|\z)').Groups['body'].Value
}

function Assert-Publication([string] $Workflow) {
    $publish = Get-Job $Workflow 'publish'
    $guard = $publish.IndexOf('run: ./scripts/assert-release-readiness.ps1 -Sha $env:GITHUB_SHA', [StringComparison]::Ordinal)
    Assert-Contract ($guard -ge 0 -and $guard -lt $publish.IndexOf('name: Push to NuGet.org', [StringComparison]::Ordinal)) 'Publication must verify exact-revision release evidence before NuGet push.'
    Assert-Contract ($publish.Contains('GITHUB_TOKEN: ${{ github.token }}') -and $publish.Contains('persist-credentials: false')) 'Publication preflight needs read-only API access and a credential-free checkout.'
    Assert-Contract ($publish -notmatch 'continue-on-error:|if: always\(') 'Publication must not bypass failed evidence.'
    Assert-Contract ($Workflow.Contains('uses: ./.github/workflows/ci.yml') -and $publish.Contains('needs: ci') -and $publish -notmatch 'dotnet (build|pack) ') 'Publication must retain full CI and publish its package without rebuilding.'
}

function Assert-Campaign([string] $Workflow, [string] $Kind) {
    $matrixJob = if ($Kind -ceq 'fuzz') { 'fuzz' } else { 'test' }
    Assert-Contract ($Workflow.Contains('run-name: "Validation: ' + $Kind + ' ${{ github.event.workflow_run.head_sha || github.sha }}"')) 'Campaign titles must identify the tested revision before results exist.'
    $record = Get-Job $Workflow 'release-evidence'
    Assert-Contract ($record.Contains('name: Record release evidence') -and $record.Contains("needs: [prerequisite, $matrixJob]") -and $record.Contains('if: ${{ success() && needs.prerequisite.outputs.run == ''true'' }}')) 'Only complete successful matrix execution may record release evidence.'
    Assert-Contract ($record.Contains('persist-credentials: false') -and $record.Contains('ref: ${{ env.JDW_VALIDATION_SHA }}') -and $record.Contains("run: ./scripts/write-release-evidence.ps1 -Kind $Kind")) 'Release evidence must inspect the immutable checked-out revision.'
    Assert-Contract ($record.Contains('name: release-evidence-' + $Kind + '-${{ env.JDW_VALIDATION_SHA }}') -and $record.Contains('path: artifacts/release-evidence/receipt.json') -and $record.Contains('if-no-files-found: error') -and $record.Contains('overwrite: true') -and $record.Contains('retention-days: 30')) 'Release receipts must retain exact identity, replace stale attempts and refuse missing output.'
    if ($Kind -ceq 'portability') {
        $test = Get-Job $Workflow 'test'
        Assert-Contract ($test.Contains('./scripts/assert-test-results.ps1 -ResultsDirectory artifacts/test-results/portability-${{ matrix.os }}-${{ matrix.tf }}')) 'Portability must reject unknown skip and explicit-test outcomes before recording success.'
    }
}

function Assert-Refusal([scriptblock] $Action, [string] $Expected) {
    $caught = $null
    try { & $Action } catch { $caught = $_.Exception.Message }
    Assert-Contract ($caught -and $caught.Contains($Expected)) "Expected refusal '$Expected', got '$caught'."
}

$publish = [IO.File]::ReadAllText((Join-Path $repository '.github/workflows/publish.yml')).Replace("`r`n", "`n")
Assert-Publication $publish
Assert-Refusal { Assert-Publication ($publish.Replace('run: ./scripts/assert-release-readiness.ps1 -Sha $env:GITHUB_SHA', 'run: echo unchecked')) } 'verify exact-revision'
foreach ($kind in 'fuzz', 'portability') {
    $workflow = [IO.File]::ReadAllText((Join-Path $repository ".github/workflows/$kind.yml")).Replace("`r`n", "`n")
    Assert-Campaign $workflow $kind
    Assert-Refusal { Assert-Campaign ($workflow.Replace('success() && ', 'always() && ')) $kind } 'complete successful matrix'
    Assert-Refusal { Assert-Campaign ($workflow.Replace('ref: ${{ env.JDW_VALIDATION_SHA }}', 'ref: main')) $kind } 'immutable checked-out'
    Assert-Refusal { Assert-Campaign ($workflow.Replace('overwrite: true', 'overwrite: false')) $kind } 'replace stale attempts'
    Assert-Refusal { Assert-Campaign ($workflow.Replace('retention-days: 30', 'retention-days: 1')) $kind } 'retain exact identity'
}
'Release evidence workflow contracts passed.'
