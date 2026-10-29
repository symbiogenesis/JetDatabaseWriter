# Guard the hosted build coverage and the handoff of already validated test binaries.
# This reads the established CI matrix structure; it does not attempt to parse arbitrary YAML.
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

function Get-AnalyzerLanes([string] $Analyze) {
    $pattern = '(?ms)^          - name:[ \t]*(?<name>[^\n]+)\n(?<body>.*?)(?=^          - name:|^    steps:|\z)'
    foreach ($match in [regex]::Matches($Analyze, $pattern)) {
        $lane = @{ name = $match.Groups['name'].Value.Trim() }
        foreach ($field in [regex]::Matches($match.Groups['body'].Value, '(?m)^            (?<key>[\w-]+):[ \t]*(?<value>[^\n]+)$')) {
            $lane[$field.Groups['key'].Value] = $field.Groups['value'].Value.Trim().Trim("'`"")
        }
        $lane
    }
}

function Assert-CiContract([string] $Workflow, [xml] $Solution, [xml] $BuildProps, [xml] $TestProject) {
    Assert-Contract ($Workflow.Contains('group: ci-${{ github.ref }}') -and $Workflow.Contains("cancel-in-progress: `${{ !startsWith(github.ref, 'refs/tags/') }}")) 'Superseded branch runs must be cancelled without cancelling release runs.'
    $analyze = Get-CiJob $Workflow 'analyze'
    $test = $analyze
    $lanes = @(Get-AnalyzerLanes $analyze)
    Assert-Contract ($lanes.Count -gt 0) 'The analyzer matrix is missing or its structure changed.'

    # Every solution project is analyzed: library and tests have dedicated lanes;
    # the tools solution filter covers all remaining projects and frameworks.
    $projects = @($Solution.SelectNodes('//Project[@Path]') | ForEach-Object { [IO.Path]::GetFileNameWithoutExtension($_.Path) })
    Assert-Contract ('JetDatabaseWriter' -in $projects -and 'JetDatabaseWriter.Tests' -in $projects) 'The solution must include the library and test projects.'
    $library = @($lanes | Where-Object project -eq 'src/JetDatabaseWriter')
    Assert-Contract ($library.Count -eq 1 -and -not $library[0].options) 'The library lane must analyze every library framework.'
    $tools = @($lanes | Where-Object project -eq '.github/ci-tools.slnf')
    Assert-Contract ($tools.Count -eq 1 -and $tools[0].options -eq '-p:AnalyzeExcept=JetDatabaseWriter JetDatabaseWriter.Tests') 'The tools lane must analyze the complete tools filter except the separately analyzed library and tests.'
    $filtered = @($toolsFilter.solution.projects | ForEach-Object { [IO.Path]::GetFileNameWithoutExtension($_.Replace('\', '/')) })
    Assert-Contract (@(Compare-Object @($projects | Where-Object { $_ -notin 'JetDatabaseWriter', 'JetDatabaseWriter.Tests' }) @($filtered | Where-Object { $_ -ne 'JetDatabaseWriter' })).Count -eq 0) 'Every remaining solution project must belong to the tools filter.'

    $frameworks = @($TestProject.SelectNodes('/Project/PropertyGroup/TargetFrameworks') | ForEach-Object { $_.InnerText.Split(';', [StringSplitOptions]::RemoveEmptyEntries) } | Sort-Object -Unique)
    Assert-Contract ($frameworks.Count -gt 0) 'The test project framework declaration is missing.'
    $testLanes = @($lanes | Where-Object project -eq 'tests/JetDatabaseWriter.Tests')
    Assert-Contract (($frameworks -join ';') -eq 'net10.0') 'Tests must build locally with the pinned .NET 10 SDK.'
    Assert-Contract ($testLanes.Count -eq 3) 'Native .NET 10, netstandard and .NET 11 must have analyzer lanes.'
    foreach ($framework in 'net10.0', 'net10.0-netstandard', 'net11.0') {
        $expectedOptions = '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests'
        if ($framework -eq 'net10.0-netstandard') { $expectedOptions += ' -p:TestNetStandard=true' }
        $lane = @($testLanes | Where-Object tf -eq $framework)
        Assert-Contract ($lane.Count -eq 1 -and $lane[0].options -eq $expectedOptions -and $lane[0].runner -eq 'true') "The '$framework' test lane must analyze and publish its matching runner."
        if ($framework -eq 'net10.0-netstandard') {
            Assert-Contract ($lane[0]['restore-options'] -eq '-p:TestNetStandard=true') 'The netstandard lane must restore its matching dependency graph.'
        }
    }
    Assert-Contract ($lanes.Count -eq 5) 'Unexpected analyzer lanes require a coverage review.'

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
    Assert-Contract ($upload -match '(?m)^        if: matrix.runner$' -and $upload.Contains($runnerName) -and $upload.Contains($runnerPath) -and $upload -match 'uses: actions/upload-artifact@[0-9a-f]{40}(?:\s|$)' -and $upload.Contains('if-no-files-found: error')) 'Analyzer jobs must publish the complete matching test runner with a pinned action.'
    foreach ($name in 'analyze', 'aot', 'encryption-replacement') {
        $laneJob = Get-CiJob $Workflow $name
        Assert-Contract ($laneJob -match '(?m)^    needs: changes$' -and [regex]::Matches($laneJob, '(?m)^    needs:').Count -eq 1) 'Test analyzer lanes must start independently of other analyzer lanes.'
        Assert-Contract ($laneJob -match "(?m)^    if: needs\.changes\.outputs\.dotnet == 'true'$" -and [regex]::Matches($laneJob, '(?m)^    if:').Count -eq 1) 'All .NET jobs must use the same reviewed lane selection.'
    }
    $testSteps = [regex]::Matches($test, '(?ms)^      - name: Test\n(?<step>.*?)(?=^      - |\z)')
    Assert-Contract ($testSteps.Count -eq 1) 'Every test analyzer lane must execute the ordinary suite exactly once.'
    $testStep = $testSteps[0].Groups['step'].Value
    Assert-Contract ($testStep -match '(?m)^        if: matrix.runner$') 'Every test analyzer lane must run its full ordinary suite.'
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
    Assert-Contract ($testCommand.Contains('--test-modules JetDatabaseWriter.Tests.dll') -and $testCommand.Contains('--root-directory tests/JetDatabaseWriter.Tests/bin/Release/net10.0') -and $testCommand -match '(?:^| )--minimum-expected-tests 1(?: |$)') 'Ordinary tests must launch the validated module and reject empty discovery.'
    Assert-Contract ($testCommand -notmatch '(?:^| )--(?:filter|list-tests|explicit|project|solution)\b') 'Ordinary test jobs must run the full ordinary suite from the downloaded module.'
    Assert-Contract ($testStep.Contains('JDW_GOLDEN_OUTPUT_DIRECTORY: ${{ github.workspace }}/artifacts/golden/${{ matrix.tf }}')) 'Ordinary tests must retain normalized golden specimens in their framework lane.'
    $golden = [regex]::Match($analyze, '(?ms)^      - name: Upload golden diagnostics\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($golden.Contains('if: ${{ !cancelled() && matrix.runner }}') -and $golden.Contains('name: golden-bytes-${{ matrix.tf }}') -and $golden.Contains('path: artifacts/golden/${{ matrix.tf }}/')) 'Golden diagnostics must be uploaded after failing tests as well as successful tests.'
    Assert-Contract ($Workflow -notmatch '(?i)infersharp') 'InferSharp must remain in its independent workflow.'

    $changes = Get-CiJob $Workflow 'changes'
    Assert-Contract ($changes.Contains('fetch-depth: 0') -and $changes.Contains('run: ./scripts/select-ci-lanes.ps1') -and $changes.Contains('GITHUB_TOKEN: ${{ github.token }}') -and $changes -notmatch '(?m)^    (?:if|needs):') 'Path selection must inspect complete history and run unconditionally.'
    Assert-Contract ($Workflow.Contains("  workflow_call:`n    inputs:`n      full:") -and $Workflow.Contains('        default: true') -and $changes.Contains('JDW_CI_FORCE_FULL: ${{ inputs.full || false }}')) 'Reusable workflow calls must default to complete validation.'
    $checks = Get-CiJob $Workflow 'checks'
    Assert-Contract ($checks -notmatch '(?m)^    (?:if|needs):' -and $checks.Contains('run: ./scripts/test-scripts.ps1') -and $checks.Contains('run: ./scripts/check-security-traceability.ps1')) 'Script and security checks must remain unconditional.'
    $gate = Get-CiJob $Workflow 'gate'
    Assert-Contract ($gate.Contains('needs: [changes, checks, formal, analyze, aot, encryption-replacement]') -and $gate.Contains('if: always()') -and $gate.Contains('JDW_CI_NEEDS: ${{ toJSON(needs) }}') -and $gate.Contains('run: ./scripts/select-ci-lanes.ps1 -CheckResults')) 'The aggregate gate must reject failure, cancellation and missing selected jobs.'
}

function Assert-Scenario([string] $Name, [string] $Workflow, [string] $ExpectedError, [xml] $Props = $buildProps, [xml] $Tests = $testProject) {
    $caught = $null
    try { Assert-CiContract $Workflow $solution $Props $Tests } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        Assert-Contract ($caught -and $caught.Contains($ExpectedError)) "$Name failed: expected '$ExpectedError', got '$caught'."
    } else {
        Assert-Contract (-not $caught) "$Name failed: $caught"
    }
    "PASS: $Name"
}

Assert-Scenario 'hosted coverage and immediate validated binary execution' $workflow
Assert-Scenario 'obsolete branch runs block current validation' ($workflow.Replace("cancel-in-progress: `${{ !startsWith(github.ref, 'refs/tags/') }}", 'cancel-in-progress: false')) 'Superseded branch runs must be cancelled'
Assert-Scenario 'incomplete solution analysis' ($workflow.Replace('project: .github/ci-tools.slnf', 'project: tests/JetDatabaseWriter.TestSupport')) 'The tools lane must analyze the complete tools filter'
Assert-Scenario 'Debug analyzer build' ($workflow.Replace('dotnet build ${{ matrix.project }} -c Release', 'dotnet build ${{ matrix.project }} -c Debug')) 'The analyzer build must use Release'
Assert-Scenario 'disabled analyzers' ($workflow.Replace('dotnet build ${{ matrix.project }}', 'dotnet build -p:RunAnalyzers=false ${{ matrix.project }}')) 'Analyzer jobs must retain all strict build guarantees'
Assert-Scenario 'ordinary suite disabled' ($workflow.Replace("      - name: Test`n        if: matrix.runner", "      - name: Test`n        if: false")) 'Every test analyzer lane must run its full ordinary suite'
Assert-Scenario 'unrelated analyzer barrier' ($workflow.Replace("    name: Analyze", "    needs: another-analyzer`n    name: Analyze")) 'Test analyzer lanes must start independently'
Assert-Scenario 'dotnet lane selection disabled' ($workflow.Replace("if: needs.changes.outputs.dotnet == 'true'", 'if: false')) 'All .NET jobs must use the same reviewed lane selection'
Assert-Scenario 'missing aggregate gate' ($workflow.Replace('  gate:', '  removed:')) "The 'gate' CI job is missing"
Assert-Scenario 'aggregate ignores skipped prerequisites' ($workflow.Replace('if: always()', 'if: success()')) 'The aggregate gate must reject failure'

$analyzeJob = Get-CiJob $workflow 'analyze'
$testStep = [regex]::Match($analyzeJob, '(?ms)^      - name: Test\n.*?(?=^      - |\z)').Value
Assert-Scenario 'missing ordinary suite' ($workflow.Replace($testStep, '')) 'Every test analyzer lane must execute the ordinary suite exactly once'
Assert-Scenario 'duplicate ordinary suite' ($workflow.Replace($testStep, $testStep + $testStep)) 'Every test analyzer lane must execute the ordinary suite exactly once'
Assert-Scenario 'tests before validation' ($workflow.Replace($testStep, '').Replace('      - name: Build', $testStep + '      - name: Build')) 'Ordinary tests must follow their successful Release analyzer build'
Assert-Scenario 'redundant ordinary test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: dotnet build tests/JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the validated binaries without rebuilding'
Assert-Scenario 'redundant multiline test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: >-`n          dotnet build tests/JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the validated binaries without rebuilding'
Assert-Scenario 'empty test discovery accepted' ($workflow.Replace('--minimum-expected-tests 1', '--minimum-expected-tests 0')) 'Ordinary tests must launch the validated module and reject empty discovery'
Assert-Scenario 'partial ordinary test selection' ($workflow.Replace('--minimum-expected-tests 1', '--minimum-expected-tests 1 --filter-class Example')) 'Ordinary test jobs must run the full ordinary suite'
Assert-Scenario 'golden evidence disabled' ($workflow.Replace('JDW_GOLDEN_OUTPUT_DIRECTORY:', 'REMOVED_GOLDEN_OUTPUT_DIRECTORY:')) 'Ordinary tests must retain normalized golden specimens'
Assert-Scenario 'golden evidence only on success' ($workflow.Replace('if: ${{ !cancelled() && matrix.runner }}', 'if: matrix.runner')) 'Golden diagnostics must be uploaded after failing tests'

$weakenedProps = [xml]$buildProps.OuterXml
$weakenedProps.SelectSingleNode('/Project/PropertyGroup/TreatWarningsAsErrors').InnerText = 'false'
Assert-Scenario 'warnings cease to be errors' $workflow "The 'TreatWarningsAsErrors' build guarantee must remain enabled" $weakenedProps
$additionalFramework = [xml]$testProject.OuterXml
$additionalFramework.SelectSingleNode('/Project/PropertyGroup/TargetFrameworks').InnerText += ';net9.0'
Assert-Scenario 'new test framework lacks hosted coverage' $workflow 'Tests must build locally with the pinned .NET 10 SDK' -Tests $additionalFramework
$suppressedWarning = [xml]$testProject.OuterXml
$suppressedWarning.SelectSingleNode('/Project/PropertyGroup/NoWarn').InnerText += ';CA2000'
Assert-Scenario 'blanket suppression hides disposal analysis' $workflow 'The test project must not add blanket warning suppressions' -Tests $suppressedWarning

$standardLane = '(?ms)^          - name: tests net10\.0 netstandard\n.*?(?=^          - name:|^    steps:|\z)'
Assert-Scenario 'missing standard asset lane' ([regex]::Replace($workflow, $standardLane, '')) 'Native .NET 10, netstandard and .NET 11 must have analyzer lanes'
Assert-Scenario 'wrong standard analyzer project' ($workflow.Replace('-p:AnalyzeOnly=JetDatabaseWriter.Tests -p:TestNetStandard=true', '-p:AnalyzeOnly=JetDatabaseWriter.Test -p:TestNetStandard=true')) "The 'net10.0-netstandard' test lane must analyze"
Assert-Scenario 'mismatched runtime runner' ($workflow.Replace("tf: net11.0`n", "tf: net10.0`n")) "The 'net10.0' test lane must analyze"
Assert-Scenario 'standard graph not restored' ($workflow.Replace('restore-options: -p:TestNetStandard=true', 'restore-options: removed')) 'The netstandard lane must restore its matching dependency graph'
Assert-Scenario 'missing prerelease installation' ($workflow.Replace('global-json-file: .github/dotnet11/global.json', 'global-json-file: global.json')) 'The .NET 11 lane must install'
Assert-Scenario 'runtime stays on installed 10' ($workflow.Replace("'LatestMajor'", "'Major'")) 'Test lanes must select and verify their intended runtime'
Assert-Scenario 'prerelease runtime excluded' ($workflow.Replace("DOTNET_ROLL_FORWARD_TO_PRERELEASE:", 'REMOVED_PRERELEASE:')) 'Test lanes must select and verify their intended runtime'
Assert-Scenario 'runtime assertion disabled' ($workflow.Replace('JDW_EXPECTED_RUNTIME_MAJOR:', 'REMOVED_RUNTIME_ASSERTION:')) 'Test lanes must select and verify their intended runtime'

Assert-Scenario 'InferSharp added to ordinary CI' ($workflow + '
# InferSharp') 'InferSharp must remain in its independent workflow.'
