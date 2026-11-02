# Guard the independent hosted fuzz gate, dependency cache and retained reproductions.
[CmdletBinding()]
param([string] $WorkflowPath)

$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $WorkflowPath) { $WorkflowPath = Join-Path $repository '.github/workflows/fuzz.yml' }
$workflow = [IO.File]::ReadAllText($WorkflowPath).Replace("`r`n", "`n")

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Workflow([string] $Workflow) {
    $prerequisite = [regex]::Match($Workflow, '(?ms)^  prerequisite:\n(?<job>.*?)(?=^  [\w-]+:\n|\z)').Groups['job'].Value
    $fuzz = [regex]::Match($Workflow, '(?ms)^  fuzz:\n(?<job>.*?)(?=^  [\w-]+:\n|\z)').Groups['job'].Value
    Assert-Contract ($Workflow.Contains("  workflow_run:`n    workflows: [CI]`n    types: [completed]`n    branches: [main]") -and $Workflow.Contains('  workflow_dispatch:')) 'Fuzzing must run automatically after main CI and retain manual dispatch.'
    Assert-Contract ($prerequisite.Contains("github.ref == 'refs/heads/main'") -and $prerequisite.Contains("github.event.workflow_run.conclusion == 'success'") -and $prerequisite.Contains("github.event.workflow_run.head_branch == 'main'") -and $prerequisite.Contains('github.event.workflow_run.head_repository.full_name == github.repository') -and $prerequisite.Contains("github.event.workflow_run.path == '.github/workflows/ci.yml'") -and $prerequisite.Contains("github.event.workflow_run.event == 'push' || github.event.workflow_run.event == 'workflow_dispatch'") -and $prerequisite.Contains('./scripts/assert-ci-success.ps1 -Sha $env:JDW_VALIDATION_SHA') -and $fuzz.Contains('needs: prerequisite')) 'Fuzzing must require successful exact-main CI from this repository before starting its runners.'
    Assert-Contract ($prerequisite.Contains("startsWith(github.event.workflow_run.path, '.github/workflows/ci.yml@')")) 'Fuzzing must accept the supported manual workflow suffix at the exact workflow boundary.'
    Assert-Contract ($Workflow.Contains('JDW_VALIDATION_SHA: ${{ github.event.workflow_run.head_sha || github.sha }}')) 'Automatic fuzzing must bind to the completed CI revision.'
    $receipt = [regex]::Match($prerequisite, '(?ms)^      - name: Download completed CI selection\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($receipt.Contains('if: github.event_name == ''workflow_run''') -and $receipt.Contains('name: ci-selection') -and $receipt.Contains('path: artifacts/prerequisite') -and $receipt.Contains('run-id: ${{ github.event.workflow_run.id }}') -and $receipt.Contains('github-token: ${{ github.token }}') -and $receipt -match 'uses: actions/download-artifact@[0-9a-f]{40}(?:\s|$)') 'Automatic fuzzing must read the content receipt from its exact successful CI run.'
    Assert-Contract ($prerequisite.Contains('run: ${{ steps.select.outputs.run }}') -and $prerequisite.Contains('id: select') -and $prerequisite.Contains('run: ./scripts/select-specialized-validation.ps1') -and $fuzz.Contains('if: needs.prerequisite.outputs.run == ''true''')) 'Automatic fuzzing must honor the reviewed .NET content selection while retaining explicit manual campaigns.'
    Assert-Contract ($Workflow.Contains('DOTNET_NOLOGO: true') -and $Workflow.Contains('DOTNET_CLI_TELEMETRY_OPTOUT: true')) 'Hosted fuzz builds must use the common .NET environment.'
    $checkouts = [regex]::Matches($Workflow, '(?ms)^      - (?:name: [^\n]+\n        )?uses: actions/checkout@[^\n]+\n(?<step>.*?)(?=^      - |\z)')
    Assert-Contract ($checkouts.Count -eq 3) 'Fuzz prerequisite, runner and evidence jobs must check out the dispatched revision.'
    foreach ($checkout in $checkouts) {
        Assert-Contract ($checkout.Groups['step'].Value.Contains('persist-credentials: false')) 'Fuzz checkouts must not persist repository credentials.'
        Assert-Contract ($checkout.Groups['step'].Value.Contains('ref: ${{ env.JDW_VALIDATION_SHA }}')) 'Every fuzz checkout must use the immutable completed CI revision.'
    }
    $setup = [regex]::Match($fuzz, '(?ms)^      - (?:name: [^\n]+\n        )?uses: actions/setup-dotnet@[^\n]+\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($setup.Contains('global-json-file: global.json') -and $setup.Contains('cache: true') -and $setup.Contains("cache-dependency-path: '**/packages.lock.json'")) 'Fuzz runners must cache their locked NuGet dependencies with the pinned SDK.'
    Assert-Contract ($fuzz.Contains('tf: [net10.0, net11.0]') -and $fuzz.Contains('JDW_EXPECTED_RUNTIME_MAJOR:') -and $fuzz.Contains('DOTNET_ROLL_FORWARD_TO_PRERELEASE:')) 'Fuzzing must retain both explicitly selected runtimes.'
    Assert-Contract ($fuzz.Contains('FUZZ_CASES: ${{ inputs.cases || ''64'' }}') -and $fuzz.Contains('FUZZ_SEED: ${{ inputs.seed || github.run_number }}')) 'Automatic fuzzing must supply a bounded case count and reproducible changing seed.'
    Assert-Contract ($fuzz.Contains('dotnet restore tests/JetDatabaseWriter.Tests --locked-mode') -and $fuzz.Contains('dotnet build tests/JetDatabaseWriter.Tests -c Release --no-restore -f net10.0 -p:RunAnalyzers=false')) 'Fuzz runners must build locked Release inputs without repeating the CI analyzers.'
    $upload = [regex]::Match($fuzz, '(?ms)^      - name: Retain reproductions and logs\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($upload.Contains('if: always()') -and $upload.Contains('name: fuzz-${{ matrix.tf }}') -and $upload.Contains('path: artifacts/fuzz/') -and $upload.Contains('if-no-files-found: error')) 'Each fuzz runtime must retain diagnostics on failure and report missing reproductions.'
}

function Assert-Refusal([string] $Name, [string] $Candidate, [string] $Expected) {
    $caught = $null
    try { Assert-Workflow $Candidate } catch { $caught = $_.Exception.Message }
    Assert-Contract ($caught -and $caught.Contains($Expected)) "$Name did not refuse for the expected reason: $caught"
    "PASS: $Name"
}

Assert-Workflow $workflow
Assert-Refusal 'missing main guard' ($workflow.Replace("github.ref == 'refs/heads/main'", 'true')) 'successful exact-main CI'
Assert-Refusal 'automatic campaign omitted' ($workflow.Replace('types: [completed]', 'types: [requested]')) 'run automatically after main CI'
Assert-Refusal 'foreign repository accepted' ($workflow.Replace('github.event.workflow_run.head_repository.full_name == github.repository', 'true')) 'from this repository'
Assert-Refusal 'failed CI accepted' ($workflow.Replace("github.event.workflow_run.conclusion == 'success'", 'true')) 'successful exact-main CI'
Assert-Refusal 'workflow prefix boundary lost' ($workflow.Replace("'.github/workflows/ci.yml@'", "'.github/workflows/ci.yml'")) 'exact workflow boundary'
Assert-Refusal 'mutable branch checkout' ($workflow.Replace('ref: ${{ env.JDW_VALIDATION_SHA }}', 'ref: main')) 'immutable completed CI revision'
Assert-Refusal 'wrong revision preflight' ($workflow.Replace(' -Sha $env:JDW_VALIDATION_SHA', '')) 'successful exact-main CI'
Assert-Refusal 'automatic case count missing' ($workflow.Replace('FUZZ_CASES: ${{ inputs.cases || ''64'' }}', 'FUZZ_CASES: ${{ inputs.cases }}')) 'bounded case count'
Assert-Refusal 'receipt taken from a different run' ($workflow.Replace('run-id: ${{ github.event.workflow_run.id }}', 'run-id: ${{ github.run_id }}')) 'receipt from its exact successful CI run'
Assert-Refusal 'content selection bypassed' ($workflow.Replace('if: needs.prerequisite.outputs.run == ''true''', 'if: true')) 'honor the reviewed .NET content selection'
Assert-Refusal 'content selection helper omitted' ($workflow.Replace('run: ./scripts/select-specialized-validation.ps1', 'run: echo run=true')) 'honor the reviewed .NET content selection'
Assert-Refusal 'missing prerequisite' ($workflow.Replace('needs: prerequisite', 'needs: removed')) 'successful exact-main CI'
Assert-Refusal 'persisted credentials' ($workflow.Replace('persist-credentials: false', 'persist-credentials: true')) 'persist repository credentials'
Assert-Refusal 'uncached dependencies' ($workflow.Replace('cache: true', 'cache: false')) 'cache their locked NuGet dependencies'
Assert-Refusal 'unlocked dependency cache' ($workflow.Replace("cache-dependency-path: '**/packages.lock.json'", "cache-dependency-path: '**/*.csproj'")) 'cache their locked NuGet dependencies'
Assert-Refusal 'repeated analyzers' ($workflow.Replace(' -p:RunAnalyzers=false', '')) 'without repeating the CI analyzers'
Assert-Refusal 'missing runtime leg' ($workflow.Replace('tf: [net10.0, net11.0]', 'tf: [net10.0]')) 'both explicitly selected runtimes'
Assert-Refusal 'lost failure diagnostics' ($workflow.Replace('if: always()', 'if: success()')) 'retain diagnostics on failure'
Assert-Refusal 'missing reproductions ignored' ($workflow.Replace('if-no-files-found: error', 'if-no-files-found: warn')) 'report missing reproductions'
Write-Output 'Hosted fuzz workflow contracts passed.'
