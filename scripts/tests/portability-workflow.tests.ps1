# Keep automated portability coverage bound to the successful prerequisite main CI run.
[CmdletBinding()]
param([string] $WorkflowPath)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
if (-not $WorkflowPath) { $WorkflowPath = Join-Path $repoRoot '.github/workflows/portability.yml' }
$workflow = [IO.File]::ReadAllText($WorkflowPath).Replace("`r`n", "`n")

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Get-WorkflowJob([string] $Workflow, [string] $Name) {
    $pattern = '(?ms)^  ' + [regex]::Escape($Name) + ':\n(?<body>.*?)(?=^  [\w-]+:\n|\z)'
    $match = [regex]::Match($Workflow, $pattern)
    Assert-Contract $match.Success "The '$Name' portability job is missing."
    return $match.Groups['body'].Value
}

function Assert-Workflow([string] $Workflow) {
    Assert-Contract ($Workflow.Contains("  workflow_run:`n    workflows: [CI]`n    types: [completed]`n    branches: [main]") -and $Workflow.Contains('  workflow_dispatch:')) 'Portability must run automatically after main CI and retain manual dispatch.'
    Assert-Contract ($Workflow.Contains('group: portability-${{ github.ref }}') -and $Workflow.Contains("cancel-in-progress: false`n  queue: max")) 'Portability must queue manual requests without discarding pending runs.'
    Assert-Contract ([regex]::Matches($Workflow, '(?m)^          persist-credentials: false$').Count -eq 2) 'Portability checkouts must not persist credentials.'
    $prerequisite = Get-WorkflowJob $Workflow 'prerequisite'
    Assert-Contract ($prerequisite.Contains("github.ref == 'refs/heads/main'") -and $prerequisite.Contains("github.event.workflow_run.conclusion == 'success'") -and $prerequisite.Contains("github.event.workflow_run.head_branch == 'main'") -and $prerequisite.Contains('github.event.workflow_run.head_repository.full_name == github.repository') -and $prerequisite.Contains("github.event.workflow_run.path == '.github/workflows/ci.yml'") -and $prerequisite.Contains("github.event.workflow_run.event == 'push' || github.event.workflow_run.event == 'workflow_dispatch'") -and $prerequisite.Contains('./scripts/assert-ci-success.ps1 -Sha $env:JDW_VALIDATION_SHA') -and $prerequisite.Contains('GITHUB_TOKEN: ${{ github.token }}')) 'Portability must require completed exact-main CI from this repository before its expensive jobs.'
    Assert-Contract ($prerequisite.Contains("startsWith(github.event.workflow_run.path, '.github/workflows/ci.yml@')")) 'Portability must accept the supported manual workflow suffix at the exact workflow boundary.'
    Assert-Contract ($Workflow.Contains('JDW_VALIDATION_SHA: ${{ github.event.workflow_run.head_sha || github.sha }}') -and [regex]::Matches($Workflow, '(?m)^          ref: \$\{\{ env.JDW_VALIDATION_SHA \}\}$').Count -eq 2) 'Every portability checkout must use the immutable completed CI revision.'
    $receipt = [regex]::Match($prerequisite, '(?ms)^      - name: Download completed CI selection\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($receipt.Contains('if: github.event_name == ''workflow_run''') -and $receipt.Contains('name: ci-selection') -and $receipt.Contains('path: artifacts/prerequisite') -and $receipt.Contains('run-id: ${{ github.event.workflow_run.id }}') -and $receipt.Contains('github-token: ${{ github.token }}') -and $receipt -match 'uses: actions/download-artifact@[0-9a-f]{40}(?:\s|$)') 'Automatic portability must read the content receipt from its exact successful CI run.'
    $test = Get-WorkflowJob $Workflow 'test'
    Assert-Contract ($test.Contains('needs: prerequisite') -and $test.Contains('fail-fast: false')) 'Portability must retain independent coverage behind its CI prerequisite.'
    Assert-Contract ($prerequisite.Contains('run: ${{ steps.select.outputs.run }}') -and $prerequisite.Contains('id: select') -and $prerequisite.Contains('run: ./scripts/select-specialized-validation.ps1') -and $test.Contains('if: needs.prerequisite.outputs.run == ''true''')) 'Automatic portability must honor the reviewed .NET content selection while retaining explicit manual campaigns.'
    Assert-Contract ($test.Contains('os: [ubuntu-latest, macos-latest]') -and $test.Contains('tf: [net10.0, net11.0]') -and -not $test.Contains('windows-latest')) 'Portability must cover Linux and macOS on both runtimes without repeating prerequisite Windows tests.'
    Assert-Contract ($test.Contains('dotnet restore tests/JetDatabaseWriter.Tests --locked-mode') -and $test.Contains('dotnet build tests/JetDatabaseWriter.Tests -c Release --no-restore -f net10.0 -p:RunAnalyzers=false')) 'Portability must build locked Release binaries without repeating prerequisite analyzers.'
    Assert-Contract ($test.Contains('global-json-file: .github/dotnet11/global.json') -and $test.Contains("JDW_EXPECTED_RUNTIME_MAJOR: `${{ matrix.tf == 'net11.0' && '11' || '10' }}") -and $test.Contains("DOTNET_ROLL_FORWARD: `${{ matrix.tf == 'net11.0' && 'LatestMajor' || 'LatestPatch' }}") -and $test.Contains("DOTNET_ROLL_FORWARD_TO_PRERELEASE: `${{ matrix.tf == 'net11.0' && '1' || '0' }}")) 'Portability must install, select and verify each intended runtime.'
    Assert-Contract ($test.Contains('dotnet test --test-modules JetDatabaseWriter.Tests.dll') -and $test.Contains('--root-directory tests/JetDatabaseWriter.Tests/bin/Release/net10.0') -and $test.Contains('--minimum-expected-tests 48') -and $test -notmatch '--filter[- ]') 'Portability must run the complete built suite with a nonempty discovery guard.'
    Assert-Contract ($test.Contains('timeout-minutes: 90') -and $test.Contains('--timeout 80m')) 'Portability must allow bounded exhaustive fault sweeps to finish and retain diagnostics.'
    Assert-Contract ($test.Contains('--report-xunit-xml --report-xunit-xml-filename tests.xml') -and $test.Contains('--results-directory artifacts/test-results/portability-${{ matrix.os }}-${{ matrix.tf }}')) 'Portability must collect unique xUnit results for every OS and runtime.'
    $results = [regex]::Match($test, '(?ms)^      - name: Upload test results\n.*?(?=^      - |\z)').Value
    Assert-Contract ($results.Contains('if: ${{ !cancelled() }}') -and $results.Contains('name: portability-results-${{ matrix.os }}-${{ matrix.tf }}') -and $results.Contains('path: artifacts/test-results/portability-${{ matrix.os }}-${{ matrix.tf }}/')) 'Portability must retain unique test artifacts after failed tests.'
    $golden = [regex]::Match($test, '(?ms)^      - name: Upload golden diagnostics\n.*?(?=^      - |\z)').Value
    Assert-Contract ($test.Contains('JDW_GOLDEN_OUTPUT_DIRECTORY: ${{ github.workspace }}/artifacts/golden/portability-${{ matrix.os }}-${{ matrix.tf }}') -and $golden.Contains('if: ${{ !cancelled() }}') -and $golden.Contains('name: portability-golden-${{ matrix.os }}-${{ matrix.tf }}') -and $golden.Contains('path: artifacts/golden/portability-${{ matrix.os }}-${{ matrix.tf }}/')) 'Portability must retain unique golden byte diagnostics after failed tests.'
    Assert-Contract ($Workflow -notmatch 'continue-on-error:') 'Portability failures must remain failures.'
}

function Assert-Scenario([string] $Name, [string] $Workflow, [string] $ExpectedError = '') {
    $caught = $null
    try { Assert-Workflow $Workflow } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        Assert-Contract ($caught -and $caught.Contains($ExpectedError)) "$Name expected '$ExpectedError', got '$caught'."
    } else { Assert-Contract (-not $caught) "$Name failed: $caught" }
    "PASS: $Name"
}

Assert-Scenario 'complementary portability workflow' $workflow
Assert-Scenario 'pending manual runs discarded' ($workflow.Replace('  queue: max', '  queue: single')) 'queue manual requests'
Assert-Scenario 'checkout credentials retained' ($workflow.Replace('persist-credentials: false', 'persist-credentials: true')) 'checkouts must not persist credentials'
Assert-Scenario 'missing prerequisite guard' ($workflow.Replace("github.ref == 'refs/heads/main'", 'false')) 'completed exact-main CI'
Assert-Scenario 'automatic campaign omitted' ($workflow.Replace('types: [completed]', 'types: [requested]')) 'run automatically after main CI'
Assert-Scenario 'foreign repository accepted' ($workflow.Replace('github.event.workflow_run.head_repository.full_name == github.repository', 'true')) 'from this repository'
Assert-Scenario 'failed CI accepted' ($workflow.Replace("github.event.workflow_run.conclusion == 'success'", 'true')) 'completed exact-main CI'
Assert-Scenario 'workflow prefix boundary lost' ($workflow.Replace("'.github/workflows/ci.yml@'", "'.github/workflows/ci.yml'")) 'exact workflow boundary'
Assert-Scenario 'mutable branch checkout' ($workflow.Replace('ref: ${{ env.JDW_VALIDATION_SHA }}', 'ref: main')) 'immutable completed CI revision'
Assert-Scenario 'wrong revision preflight' ($workflow.Replace(' -Sha $env:JDW_VALIDATION_SHA', '')) 'completed exact-main CI'
Assert-Scenario 'receipt taken from a different run' ($workflow.Replace('run-id: ${{ github.event.workflow_run.id }}', 'run-id: ${{ github.run_id }}')) 'receipt from its exact successful CI run'
Assert-Scenario 'content selection bypassed' ($workflow.Replace('if: needs.prerequisite.outputs.run == ''true''', 'if: true')) 'honor the reviewed .NET content selection'
Assert-Scenario 'content selection helper omitted' ($workflow.Replace('run: ./scripts/select-specialized-validation.ps1', 'run: echo run=true')) 'honor the reviewed .NET content selection'
Assert-Scenario 'repeated Windows coverage' ($workflow.Replace('os: [ubuntu-latest, macos-latest]', 'os: [windows-latest, ubuntu-latest, macos-latest]')) 'without repeating prerequisite Windows tests'
Assert-Scenario 'missing runtime coverage' ($workflow.Replace('tf: [net10.0, net11.0]', 'tf: [net10.0]')) 'cover Linux and macOS on both runtimes'
Assert-Scenario 'repeated analyzer work' ($workflow.Replace(' -p:RunAnalyzers=false', '')) 'without repeating prerequisite analyzers'
Assert-Scenario 'missing runtime guard' ($workflow.Replace('JDW_EXPECTED_RUNTIME_MAJOR:', 'REMOVED_RUNTIME:')) 'select and verify each intended runtime'
Assert-Scenario 'suite narrowed unexpectedly' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 48 --filter-class Example')) 'complete built suite'
Assert-Scenario 'empty discovery accepted' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 0')) 'nonempty discovery guard'
Assert-Scenario 'exhaustive sweeps killed early' ($workflow.Replace('timeout-minutes: 90', 'timeout-minutes: 30')) 'bounded exhaustive fault sweeps'
Assert-Scenario 'missing xUnit results' ($workflow.Replace('--report-xunit-xml --report-xunit-xml-filename tests.xml', '')) 'collect unique xUnit results'
Assert-Scenario 'results collide across hosts' ($workflow.Replace('name: portability-results-${{ matrix.os }}-${{ matrix.tf }}', 'name: portability-results')) 'retain unique test artifacts'
Assert-Scenario 'failed test evidence dropped' ($workflow.Replace('if: ${{ !cancelled() }}', 'if: success()')) 'retain unique test artifacts after failed tests'
Assert-Scenario 'missing golden diagnostics' ($workflow.Replace('JDW_GOLDEN_OUTPUT_DIRECTORY:', 'REMOVED_GOLDEN:')) 'retain unique golden byte diagnostics'
