# Guard the hosted build coverage and the handoff of already validated test binaries.
# Coverage expectations stay independent of the shared matrix definition.
[CmdletBinding()]
param([string] $WorkflowPath)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
if (-not $WorkflowPath) { $WorkflowPath = Join-Path $repoRoot '.github/workflows/ci.yml' }
$workflow = [IO.File]::ReadAllText($WorkflowPath).Replace("`r`n", "`n")
$solution = [xml][IO.File]::ReadAllText((Join-Path $repoRoot 'JetDatabaseWriter.slnx'))
$buildProps = [xml][IO.File]::ReadAllText((Join-Path $repoRoot 'Directory.Build.props'))
$testProject = [xml][IO.File]::ReadAllText((Join-Path $repoRoot 'tests/JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj'))
$toolsFilter = [IO.File]::ReadAllText((Join-Path $repoRoot '.github/ci-tools.slnf')) | ConvertFrom-Json
. (Join-Path $repoRoot 'scripts/ci-matrix.ps1')
$matrix = (Get-CiAnalyzerMatrix).include

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Get-CiJob([string] $Workflow, [string] $Name) {
    $jobs = [regex]::Match($Workflow, '(?ms)^jobs:\n(?<jobs>.*)$').Groups['jobs'].Value
    $pattern = '(?ms)^  ' + [regex]::Escape($Name) + ':\n(?<body>.*?)(?=^  [\w-]+:\n|\z)'
    $match = [regex]::Match($jobs, $pattern)
    Assert-Contract $match.Success "The '$Name' CI job is missing or its structure changed."
    return $match.Groups['body'].Value
}

function Assert-CiContract([string] $Workflow, [xml] $Solution, [xml] $BuildProps, [xml] $TestProject, [object[]] $Lanes) {
    Assert-Contract ($Workflow.Contains('group: ci-${{ github.ref }}') -and $Workflow.Contains("cancel-in-progress: `${{ !startsWith(github.ref, 'refs/tags/') }}")) 'Superseded branch runs must be cancelled without cancelling release runs.'
    $analyze = Get-CiJob $Workflow 'analyze'
    $test = $analyze
    Assert-Contract ($analyze.Contains('matrix: ${{ fromJSON(needs.changes.outputs.matrix) }}') -and [regex]::Matches($analyze, '(?m)^      matrix:').Count -eq 1) 'The analyzer matrix must consume the complete shared definition.'
    $lanes = @($Lanes)

    # Every solution project is analyzed: library and tests have dedicated lanes;
    # the tools solution filter covers all remaining projects and frameworks.
    $projects = @($Solution.SelectNodes('//Project[@Path]') | ForEach-Object { [IO.Path]::GetFileNameWithoutExtension($_.Path) })
    Assert-Contract ('JetDatabaseWriter' -in $projects -and 'JetDatabaseWriter.Tests' -in $projects) 'The solution must include the library and test projects.'
    $library = @($lanes | Where-Object project -eq 'src/JetDatabaseWriter')
    Assert-Contract ($library.Count -eq 1 -and -not $library[0].options) 'The library lane must analyze every library framework.'
    $tools = @($lanes | Where-Object project -eq '.github/ci-tools.slnf')
    Assert-Contract ($tools.Count -eq 1 -and $tools[0].options -eq '"-p:AnalyzeExcept=JetDatabaseWriter JetDatabaseWriter.Tests"') 'The tools lane must analyze the complete tools filter except the separately analyzed library and tests.'
    Assert-Contract ($library[0].name -ceq 'library' -and $library[0].pack -is [bool] -and $library[0].pack -and $tools[0].name -ceq 'tools') 'Library and tools job names and package creation must match the validated ancestor inventory.'
    $filtered = @($toolsFilter.solution.projects | ForEach-Object { [IO.Path]::GetFileNameWithoutExtension($_.Replace('\', '/')) })
    Assert-Contract (@(Compare-Object @($projects | Where-Object { $_ -notin 'JetDatabaseWriter', 'JetDatabaseWriter.Tests' }) @($filtered | Where-Object { $_ -ne 'JetDatabaseWriter' })).Count -eq 0) 'Every remaining solution project must belong to the tools filter.'

    $frameworks = @($TestProject.SelectNodes('/Project/PropertyGroup/TargetFrameworks') | ForEach-Object { $_.InnerText.Split(';', [StringSplitOptions]::RemoveEmptyEntries) } | Sort-Object -Unique)
    Assert-Contract ($frameworks.Count -gt 0) 'The test project framework declaration is missing.'
    $testLanes = @($lanes | Where-Object project -eq 'tests/JetDatabaseWriter.Tests')
    Assert-Contract (($frameworks -join ';') -eq 'net10.0') 'Tests must build locally with the pinned .NET 10 SDK.'
    Assert-Contract ($testLanes.Count -eq 15) 'Native .NET 10, netstandard and .NET 11 must each have five analyzed test partitions.'
    $sweepClasses = [ordered]@{
        jet3 = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityJet3Tests'
        jet4 = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityJet4Tests'
        ace = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityAceTests'
    }
    $partitionNamespaces = 'JetDatabaseWriter.Tests.Schema JetDatabaseWriter.Tests.RoundTrip JetDatabaseWriter.Tests.Writer'
    $partitions = @('ordinary', 'schema-writer') + @($sweepClasses.Keys)
    foreach ($framework in 'net10.0', 'net10.0-netstandard', 'net11.0') {
        $expectedOptions = '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests'
        if ($framework -eq 'net10.0-netstandard') { $expectedOptions += ' -p:TestNetStandard=true' }
        foreach ($partition in $partitions) {
            $lane = @($testLanes | Where-Object { $_.tf -eq $framework -and $_.partition -eq $partition })
            Assert-Contract ($lane.Count -eq 1 -and $lane[0].options -eq $expectedOptions -and $lane[0].runner -is [bool] -and $lane[0].runner) "The '$framework/$partition' test partition must analyze and execute its matching runner exactly once."
            $name = 'tests ' + $framework.Replace('net10.0-netstandard', 'net10.0 netstandard') + ' ' + $partition
            Assert-Contract ($lane[0].name -ceq $name) 'Test partition job names must match the validated ancestor job inventory.'
            $expectedFilter = switch ($partition) {
                ordinary { '--filter-not-namespace ' + $partitionNamespaces }
                schema-writer { '--filter-namespace ' + $partitionNamespaces + ' --filter-not-class ' + ($sweepClasses.Values -join ' ') }
                default { '--filter-class ' + $sweepClasses[$partition] }
            }
            Assert-Contract ($lane[0]['test-options'] -ceq $expectedFilter) "The '$framework/$partition' filter must retain the exhaustive, disjoint partition of the full ordinary suite."
            if ($framework -eq 'net10.0-netstandard') {
                Assert-Contract ($lane[0]['restore-options'] -eq '-p:TestNetStandard=true') 'The netstandard lane must restore its matching dependency graph.'
            }
        }
    }
    Assert-Contract ($lanes.Count -eq 17) 'Unexpected analyzer lanes require a coverage review.'
    Assert-Contract (@($lanes.name | Sort-Object -Unique).Count -eq $lanes.Count) 'Every analyzer partition must have a unique job name.'

    $build = [regex]::Match($analyze, '(?m)^        run: (?<command>dotnet build [^\n]+)$').Groups['command'].Value
    Assert-Contract ($build -match '(?:^| )-c Release(?: |$)' -and $build.Contains('${{ matrix.project }}') -and $build.Contains('${{ matrix.options }}')) 'The analyzer build must use Release and every matrix lane option.'
    Assert-Contract ($analyze -notmatch '(?i)(RunAnalyzers|RunAnalyzersDuringBuild|EnableNETAnalyzers|EnforceCodeStyleInBuild|GenerateDocumentationFile|TreatWarningsAsErrors)\s*=\s*false|continue-on-error:') 'Analyzer jobs must retain all strict build guarantees.'
    foreach ($property in 'TreatWarningsAsErrors', 'EnableNETAnalyzers', 'EnforceCodeStyleInBuild', 'GenerateDocumentationFile') {
        $value = $BuildProps.SelectSingleNode("/Project/PropertyGroup/$property")
        Assert-Contract ($null -ne $value -and $value.InnerText -eq 'true') "The '$property' build guarantee must remain enabled."
    }
    foreach ($property in $TestProject.SelectNodes('/Project/PropertyGroup/*') | Where-Object { $_.LocalName -in @('RunAnalyzers', 'RunAnalyzersDuringBuild', 'EnableNETAnalyzers', 'EnforceCodeStyleInBuild', 'GenerateDocumentationFile', 'TreatWarningsAsErrors') -and $_.InnerText -eq 'false' }) {
        Assert-Contract ($property.Condition -eq "'`$(Configuration)' != 'Release'") 'The test project must retain strict Release analysis.'
    }
    $suppressed = @($TestProject.SelectNodes('/Project/PropertyGroup/NoWarn') | ForEach-Object { $_.InnerText.Split(';') } | Where-Object { $_ -and $_ -ne '$(NoWarn)' } | Sort-Object -Unique)
    Assert-Contract (($suppressed -join ';') -eq 'CA1707;SA1615') 'The test project must not add blanket warning suppressions.'

    # Each runtime leg executes its own Release analyzer output in the same job.
    # The artifact remains available for focused DAO checks on Access hosts.
    $runnerName = 'name: dao-test-runner-${{ matrix.tf }}'
    $runnerPath = 'path: tests/JetDatabaseWriter.Tests/bin/Release/net10.0/'
    $upload = [regex]::Match($analyze, '(?ms)^      - name: Upload DAO test runner\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($upload.Contains("if: matrix.runner && matrix.partition == 'ordinary'") -and $upload.Contains($runnerName) -and $upload.Contains($runnerPath) -and $upload -match 'uses: actions/upload-artifact@[0-9a-f]{40}(?:\s|$)' -and $upload.Contains('if-no-files-found: error')) 'Ordinary partitions must publish the complete matching test runner exactly once with a pinned action.'
    foreach ($name in 'analyze', 'aot', 'encryption-replacement') {
        $laneJob = Get-CiJob $Workflow $name
        Assert-Contract ($laneJob -match '(?m)^    needs: changes$' -and [regex]::Matches($laneJob, '(?m)^    needs:').Count -eq 1) 'Test analyzer lanes must start independently of other analyzer lanes.'
        Assert-Contract ($laneJob -match "(?m)^    if: needs\.changes\.outputs\.dotnet == 'true'$" -and [regex]::Matches($laneJob, '(?m)^    if:').Count -eq 1) 'All .NET jobs must use the same reviewed lane selection.'
    }
    $testSteps = [regex]::Matches($test, '(?ms)^      - name: Test\n(?<step>.*?)(?=^      - |\z)')
    Assert-Contract ($testSteps.Count -eq 1) 'Every test analyzer partition must execute its assigned tests exactly once.'
    $testStep = $testSteps[0].Groups['step'].Value
    Assert-Contract ($testStep -match '(?m)^        if: matrix.runner$') 'Every test analyzer partition must run its assigned tests.'
    $buildPosition = $test.IndexOf('      - name: Build', [StringComparison]::Ordinal)
    $testPosition = $test.IndexOf('      - name: Test', [StringComparison]::Ordinal)
    Assert-Contract ($buildPosition -ge 0 -and $buildPosition -lt $testPosition) 'Ordinary tests must follow their successful Release analyzer build.'
    $afterBuild = $test.Substring($buildPosition + '      - name: Build'.Length)
    Assert-Contract ([regex]::Matches($afterBuild, '(?m)^[ \t]+(?:run:.*\b)?dotnet[ \t]+(?:build|restore|publish)\b').Count -eq 1) 'Ordinary tests must execute the validated binaries without rebuilding.'
    Assert-Contract ($test.Contains('global-json-file: .github/dotnet11/global.json') -and $test.Contains("if: matrix.tf == 'net11.0'")) 'The .NET 11 lane must install the pinned prerelease runtime.'
    Assert-Contract ($testStep.Contains("DOTNET_ROLL_FORWARD: `${{ matrix.tf == 'net11.0' && 'LatestMajor' || 'LatestPatch' }}") -and $testStep.Contains("DOTNET_ROLL_FORWARD_TO_PRERELEASE: `${{ matrix.tf == 'net11.0' && '1' || '0' }}") -and $testStep.Contains("JDW_EXPECTED_RUNTIME_MAJOR: `${{ matrix.tf == 'net11.0' && '11' || '10' }}")) 'Test lanes must select and verify their intended runtime, including prereleases.'
    $testCommand = [regex]::Match($testStep, '(?m)^        run: (?<command>[^\n]+)$').Groups['command'].Value
    if ($testCommand -eq '>-') {
        $testCommand = ([regex]::Matches($testStep, '(?m)^          (?<line>[^\n]+)$') | ForEach-Object { $_.Groups['line'].Value.Trim() }) -join ' '
    }
    Assert-Contract ($testCommand.Contains('--test-modules JetDatabaseWriter.Tests.dll') -and $testCommand.Contains('--root-directory tests/JetDatabaseWriter.Tests/bin/Release/net10.0') -and $testCommand -match '(?:^| )--minimum-expected-tests 48(?: |$)') 'Every partition must launch the validated module and require at least all 48 fault theory rows.'
    Assert-Contract ($testCommand.Contains('${{ matrix.test-options }}') -and $testCommand -notmatch '(?:^| )--(?:filter|list-tests|explicit|project|solution)\b') 'Test partitions must use only their reviewed exhaustive matrix filters.'
    Assert-Contract ($testCommand.Contains('--results-directory artifacts/test-results/${{ matrix.tf }}-${{ matrix.partition }}')) 'Test result directories must distinguish every framework and partition.'
    Assert-Contract ($testStep.Contains('JDW_GOLDEN_OUTPUT_DIRECTORY: ${{ github.workspace }}/artifacts/golden/${{ matrix.tf }}-${{ matrix.partition }}')) 'Ordinary tests must retain normalized golden specimens in their framework partition.'
    $results = [regex]::Match($analyze, '(?ms)^      - name: Upload test results\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($results.Contains('if: ${{ !cancelled() && matrix.runner }}') -and $results.Contains('name: test-results-${{ matrix.tf }}-${{ matrix.partition }}') -and $results.Contains('path: artifacts/test-results/${{ matrix.tf }}-${{ matrix.partition }}/')) 'Every partition must retain uniquely named test results after failures as well as successes.'
    $golden = [regex]::Match($analyze, '(?ms)^      - name: Upload golden diagnostics\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($golden.Contains('if: ${{ !cancelled() && matrix.runner }}') -and $golden.Contains('name: golden-bytes-${{ matrix.tf }}-${{ matrix.partition }}') -and $golden.Contains('path: artifacts/golden/${{ matrix.tf }}-${{ matrix.partition }}/')) 'Every partition must retain uniquely named golden diagnostics after failures as well as successes.'
    Assert-Contract ($Workflow -notmatch '(?i)infersharp') 'InferSharp must remain in its independent workflow.'

    $changes = Get-CiJob $Workflow 'changes'
    Assert-Contract ($changes.Contains('matrix: ${{ steps.select.outputs.matrix }}')) 'The selection job must expose the complete shared analyzer matrix.'
    Assert-Contract ($changes.Contains('fetch-depth: 0') -and $changes.Contains('run: ./scripts/select-ci-lanes.ps1') -and $changes.Contains('GITHUB_TOKEN: ${{ github.token }}') -and $changes -notmatch '(?m)^    (?:if|needs):') 'Path selection must inspect complete history and run unconditionally.'
    Assert-Contract ($Workflow.Contains("  workflow_call:`n    inputs:`n      full:") -and $Workflow.Contains('        default: true') -and $changes.Contains('JDW_CI_FORCE_FULL: ${{ inputs.full || false }}')) 'Reusable workflow calls must default to complete validation.'
    $checks = Get-CiJob $Workflow 'checks'
    Assert-Contract ($checks -notmatch '(?m)^    (?:if|needs):' -and $checks.Contains('run: ./scripts/test-scripts.ps1') -and $checks.Contains('run: ./scripts/check-security-traceability.ps1')) 'Script and security checks must remain unconditional.'
    $gate = Get-CiJob $Workflow 'gate'
    Assert-Contract ($gate.Contains('needs: [changes, checks, formal, analyze, aot, encryption-replacement]') -and $gate.Contains('if: always()') -and $gate.Contains('JDW_CI_NEEDS: ${{ toJSON(needs) }}') -and $gate.Contains('run: ./scripts/select-ci-lanes.ps1 -CheckResults')) 'The aggregate gate must reject failure, cancellation and missing selected jobs.'
}

function Assert-Scenario([string] $Name, [string] $Workflow, [string] $ExpectedError, [xml] $Props = $buildProps, [xml] $Tests = $testProject, [object[]] $Lanes = $matrix) {
    $caught = $null
    try { Assert-CiContract $Workflow $solution $Props $Tests $Lanes } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        Assert-Contract ($caught -and $caught.Contains($ExpectedError)) "$Name failed: expected '$ExpectedError', got '$caught'."
    } else {
        Assert-Contract (-not $caught) "$Name failed: $caught"
    }
    "PASS: $Name"
}

function Assert-MatrixScenario([string] $Name, [scriptblock] $Change, [string] $ExpectedError) {
    $lanes = @($matrix | ConvertTo-Json -Depth 4 | ConvertFrom-Json -AsHashtable)
    $lanes = @(& $Change $lanes)
    Assert-Scenario $Name $workflow $ExpectedError -Lanes $lanes
}

Assert-Scenario 'hosted coverage and immediate validated binary execution' $workflow
Assert-Scenario 'obsolete branch runs block current validation' ($workflow.Replace("cancel-in-progress: `${{ !startsWith(github.ref, 'refs/tags/') }}", 'cancel-in-progress: false')) 'Superseded branch runs must be cancelled'
Assert-MatrixScenario 'incomplete solution analysis' { param($lanes) $lanes[1].project = 'tests/JetDatabaseWriter.TestSupport'; $lanes } 'The tools lane must analyze the complete tools filter'
Assert-Scenario 'shared matrix ignored' ($workflow.Replace('matrix: ${{ fromJSON(needs.changes.outputs.matrix) }}', 'matrix: {}')) 'The analyzer matrix must consume the complete shared definition'
Assert-Scenario 'shared matrix output omitted' ($workflow.Replace('matrix: ${{ steps.select.outputs.matrix }}', 'removed: value')) 'The selection job must expose the complete shared analyzer matrix'
Assert-Scenario 'Debug analyzer build' ($workflow.Replace('dotnet build ${{ matrix.project }} -c Release', 'dotnet build ${{ matrix.project }} -c Debug')) 'The analyzer build must use Release'
Assert-Scenario 'disabled analyzers' ($workflow.Replace('dotnet build ${{ matrix.project }}', 'dotnet build -p:RunAnalyzers=false ${{ matrix.project }}')) 'Analyzer jobs must retain all strict build guarantees'
Assert-Scenario 'ordinary suite disabled' ($workflow.Replace("      - name: Test`n        if: matrix.runner", "      - name: Test`n        if: false")) 'Every test analyzer partition must run its assigned tests'
Assert-Scenario 'unrelated analyzer barrier' ($workflow.Replace("    name: Analyze", "    needs: another-analyzer`n    name: Analyze")) 'Test analyzer lanes must start independently'
Assert-Scenario 'dotnet lane selection disabled' ($workflow.Replace("if: needs.changes.outputs.dotnet == 'true'", 'if: false')) 'All .NET jobs must use the same reviewed lane selection'
Assert-Scenario 'missing aggregate gate' ($workflow.Replace('  gate:', '  removed:')) "The 'gate' CI job is missing"
Assert-Scenario 'aggregate ignores skipped prerequisites' ($workflow.Replace('if: always()', 'if: success()')) 'The aggregate gate must reject failure'

$analyzeJob = Get-CiJob $workflow 'analyze'
$testStep = [regex]::Match($analyzeJob, '(?ms)^      - name: Test\n.*?(?=^      - |\z)').Value
Assert-Scenario 'missing ordinary suite' ($workflow.Replace($testStep, '')) 'Every test analyzer partition must execute its assigned tests exactly once'
Assert-Scenario 'duplicate ordinary suite' ($workflow.Replace($testStep, $testStep + $testStep)) 'Every test analyzer partition must execute its assigned tests exactly once'
Assert-Scenario 'tests before validation' ($workflow.Replace($testStep, '').Replace('      - name: Build', $testStep + '      - name: Build')) 'Ordinary tests must follow their successful Release analyzer build'
Assert-Scenario 'redundant ordinary test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: dotnet build tests/JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the validated binaries without rebuilding'
Assert-Scenario 'redundant multiline test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: >-`n          dotnet build tests/JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the validated binaries without rebuilding'
Assert-Scenario 'empty test discovery accepted' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 0')) 'Every partition must launch the validated module and require at least all 48 fault theory rows'
Assert-Scenario 'incomplete fault discovery accepted' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 47')) 'Every partition must launch the validated module and require at least all 48 fault theory rows'
Assert-Scenario 'partial ordinary test selection' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 48 --filter-class Example')) 'Test partitions must use only their reviewed exhaustive matrix filters'
Assert-Scenario 'partition filters unused' ($workflow.Replace('${{ matrix.test-options }}', '')) 'Test partitions must use only their reviewed exhaustive matrix filters'
Assert-MatrixScenario 'unexpected ordinary exclusion' { param($lanes) $lanes[2]['test-options'] += ' Example'; $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'namespace gap omits schema cases' { param($lanes) $lanes[3]['test-options'] = $lanes[3]['test-options'].Replace('JetDatabaseWriter.Tests.Schema ', ''); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'namespace overlap repeats schema cases' { param($lanes) $lanes[2]['test-options'] = $lanes[2]['test-options'].Replace('JetDatabaseWriter.Tests.Schema ', ''); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'unreviewed namespace inclusion repeats ordinary cases' { param($lanes) $lanes[3]['test-options'] = $lanes[3]['test-options'].Replace('--filter-namespace ', '--filter-namespace JetDatabaseWriter.Tests.Encryption '); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'sweep partition selects wrong class' { param($lanes) $lanes[4]['test-options'] = $lanes[4]['test-options'].Replace('StatementAtomicityJet3Tests', 'StatementAtomicityJet4Tests'); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'sweep partition disables explicit cases' { param($lanes) $lanes[4]['test-options'] += ' --explicit off'; $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'schema writer partition duplicates sweep cases' { param($lanes) $lanes[3]['test-options'] = $lanes[3]['test-options'].Replace('JetDatabaseWriter.Tests.Writer.StatementAtomicityJet3Tests ', ''); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-Scenario 'framework partitions collide in result directory' ($workflow.Replace('--results-directory artifacts/test-results/${{ matrix.tf }}-${{ matrix.partition }}', '--results-directory artifacts/test-results/${{ matrix.tf }}')) 'Test result directories must distinguish every framework and partition'
Assert-Scenario 'partition results collide in artifact name' ($workflow.Replace('name: test-results-${{ matrix.tf }}-${{ matrix.partition }}', 'name: test-results-${{ matrix.tf }}')) 'Every partition must retain uniquely named test results'
Assert-Scenario 'partition runner artifacts collide' ($workflow.Replace("if: matrix.runner && matrix.partition == 'ordinary'", 'if: matrix.runner')) 'Ordinary partitions must publish the complete matching test runner exactly once'
Assert-Scenario 'golden evidence disabled' ($workflow.Replace('JDW_GOLDEN_OUTPUT_DIRECTORY:', 'REMOVED_GOLDEN_OUTPUT_DIRECTORY:')) 'Ordinary tests must retain normalized golden specimens'
Assert-Scenario 'test result evidence only on success' ($workflow.Replace('if: ${{ !cancelled() && matrix.runner }}', 'if: matrix.runner')) 'Every partition must retain uniquely named test results'
Assert-Scenario 'golden evidence only on success' ($workflow.Replace("      - name: Upload golden diagnostics`n        if: `${{ !cancelled() && matrix.runner }}", "      - name: Upload golden diagnostics`n        if: matrix.runner")) 'Every partition must retain uniquely named golden diagnostics'
Assert-Scenario 'partition golden artifacts collide' ($workflow.Replace('name: golden-bytes-${{ matrix.tf }}-${{ matrix.partition }}', 'name: golden-bytes-${{ matrix.tf }}')) 'Every partition must retain uniquely named golden diagnostics'

$weakenedProps = [xml]$buildProps.OuterXml
$weakenedProps.SelectSingleNode('/Project/PropertyGroup/TreatWarningsAsErrors').InnerText = 'false'
Assert-Scenario 'warnings cease to be errors' $workflow "The 'TreatWarningsAsErrors' build guarantee must remain enabled" $weakenedProps
$additionalFramework = [xml]$testProject.OuterXml
$additionalFramework.SelectSingleNode('/Project/PropertyGroup/TargetFrameworks').InnerText += ';net9.0'
Assert-Scenario 'new test framework lacks hosted coverage' $workflow 'Tests must build locally with the pinned .NET 10 SDK' -Tests $additionalFramework
$suppressedWarning = [xml]$testProject.OuterXml
$suppressedWarning.SelectSingleNode('/Project/PropertyGroup/NoWarn').InnerText += ';CA2000'
Assert-Scenario 'blanket suppression hides disposal analysis' $workflow 'The test project must not add blanket warning suppressions' -Tests $suppressedWarning

Assert-MatrixScenario 'missing standard asset partition' { param($lanes) $lanes | Where-Object name -CNE 'tests net10.0 netstandard ordinary' } 'Native .NET 10, netstandard and .NET 11 must each have five analyzed test partitions'
Assert-MatrixScenario 'duplicate standard asset partition' { param($lanes) $lanes; $lanes[7] } 'Native .NET 10, netstandard and .NET 11 must each have five analyzed test partitions'
Assert-MatrixScenario 'duplicate namespace partition misses ordinary tests' { param($lanes) $lanes[2].partition = 'schema-writer'; $lanes } "The 'net10.0/ordinary' test partition must analyze"
Assert-MatrixScenario 'wrong standard analyzer project' { param($lanes) $lanes[7].options = '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Test -p:TestNetStandard=true'; $lanes } "The 'net10.0-netstandard/ordinary' test partition must analyze"
Assert-MatrixScenario 'mismatched runtime runner' { param($lanes) $lanes[12].tf = 'net10.0'; $lanes } "The 'net10.0/ordinary' test partition must analyze"
Assert-MatrixScenario 'duplicate sweep format misses another partition' { param($lanes) $lanes[6].partition = 'jet4'; $lanes } "The 'net10.0/jet4' test partition must analyze"
Assert-MatrixScenario 'unknown sweep partition omits a format' { param($lanes) $lanes[6].partition = 'other'; $lanes } "The 'net10.0/ace' test partition must analyze"
Assert-MatrixScenario 'renamed job no longer matches ancestor inventory' { param($lanes) $lanes[2].name = 'tests net10.0 renamed'; $lanes } 'Test partition job names must match the validated ancestor job inventory'
Assert-MatrixScenario 'standard graph not restored' { param($lanes) $lanes[7]['restore-options'] = 'removed'; $lanes } 'The netstandard lane must restore its matching dependency graph'
Assert-MatrixScenario 'package creation omitted' { param($lanes) $lanes[0].pack = $false; $lanes } 'Library and tools job names and package creation must match'
Assert-MatrixScenario 'package condition is a truthy string' { param($lanes) $lanes[0].pack = 'true'; $lanes } 'Library and tools job names and package creation must match'
Assert-MatrixScenario 'runner condition is a truthy string' { param($lanes) $lanes[2].runner = 'true'; $lanes } "The 'net10.0/ordinary' test partition must analyze"
Assert-Scenario 'missing prerelease installation' ($workflow.Replace('global-json-file: .github/dotnet11/global.json', 'global-json-file: global.json')) 'The .NET 11 lane must install'
Assert-Scenario 'runtime stays on installed 10' ($workflow.Replace("'LatestMajor'", "'Major'")) 'Test lanes must select and verify their intended runtime'
Assert-Scenario 'prerelease runtime excluded' ($workflow.Replace("DOTNET_ROLL_FORWARD_TO_PRERELEASE:", 'REMOVED_PRERELEASE:')) 'Test lanes must select and verify their intended runtime'
Assert-Scenario 'runtime assertion disabled' ($workflow.Replace('JDW_EXPECTED_RUNTIME_MAJOR:', 'REMOVED_RUNTIME_ASSERTION:')) 'Test lanes must select and verify their intended runtime'

Assert-Scenario 'InferSharp added to ordinary CI' ($workflow + '
# InferSharp') 'InferSharp must remain in its independent workflow.'
