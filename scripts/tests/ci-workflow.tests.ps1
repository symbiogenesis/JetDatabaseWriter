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
$testProject = [xml][IO.File]::ReadAllText((Join-Path $repoRoot 'JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj'))

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
    $analyze = Get-CiJob $Workflow 'analyze'
    $test = $analyze
    $lanes = @(Get-AnalyzerLanes $analyze)
    Assert-Contract ($lanes.Count -gt 0) 'The analyzer matrix is missing or its structure changed.'

    # Every solution project is analyzed: library and tests have dedicated lanes;
    # the solution lane covers all remaining projects and all of their frameworks.
    $projects = @($Solution.SelectNodes('//Project[@Path]') | ForEach-Object { [IO.Path]::GetFileNameWithoutExtension($_.Path) })
    Assert-Contract ('JetDatabaseWriter' -in $projects -and 'JetDatabaseWriter.Tests' -in $projects) 'The solution must include the library and test projects.'
    $library = @($lanes | Where-Object project -eq 'JetDatabaseWriter')
    Assert-Contract ($library.Count -eq 1 -and -not $library[0].options) 'The library lane must analyze every library framework.'
    $tools = @($lanes | Where-Object project -eq 'JetDatabaseWriter.slnx')
    Assert-Contract ($tools.Count -eq 1 -and $tools[0].options -eq '-p:AnalyzeExcept=JetDatabaseWriter JetDatabaseWriter.Tests') 'The tools lane must analyze the complete solution except the separately analyzed library and tests.'

    $frameworks = @($TestProject.SelectNodes('/Project/PropertyGroup/TargetFrameworks') | ForEach-Object { $_.InnerText.Split(';', [StringSplitOptions]::RemoveEmptyEntries) } | Sort-Object -Unique)
    Assert-Contract ($frameworks.Count -gt 0) 'The test project framework declaration is missing.'
    $testLanes = @($lanes | Where-Object project -eq 'JetDatabaseWriter.Tests')
    Assert-Contract ($testLanes.Count -eq $frameworks.Count) 'Every test framework must have exactly one analyzer lane.'
    foreach ($framework in $frameworks) {
        $expectedOptions = '-f ' + $framework + ' -p:AnalyzeOnly=JetDatabaseWriter.Tests'
        $lane = @($testLanes | Where-Object options -eq $expectedOptions)
        Assert-Contract ($lane.Count -eq 1) "The '$framework' test framework must have an analyzer lane."
        Assert-Contract ($lane[0].tf -eq $framework -and $lane[0].runner -eq 'true') "The '$framework' analyzer lane must publish its matching test runner."
    }
    Assert-Contract ($lanes.Count -eq (2 + $frameworks.Count)) 'Unexpected analyzer lanes require a coverage review.'

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
    $runnerPath = 'path: JetDatabaseWriter.Tests/bin/Release/${{ matrix.tf }}/'
    $upload = [regex]::Match($analyze, '(?ms)^      - name: Upload DAO test runner\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($upload -match '(?m)^        if: matrix.runner$' -and $upload.Contains($runnerName) -and $upload.Contains($runnerPath) -and $upload -match 'uses: actions/upload-artifact@[0-9a-f]{40}(?:\s|$)' -and $upload.Contains('if-no-files-found: error')) 'Analyzer jobs must publish the complete matching test runner with a pinned action.'
    Assert-Contract ($analyze -notmatch '(?m)^    needs:') 'Test analyzer lanes must start independently of other analyzer lanes.'
    $testSteps = [regex]::Matches($test, '(?ms)^      - name: Test\n(?<step>.*?)(?=^      - |\z)')
    Assert-Contract ($testSteps.Count -eq 1) 'Every test analyzer lane must execute the ordinary suite exactly once.'
    $testStep = $testSteps[0].Groups['step'].Value
    Assert-Contract ($testStep -match '(?m)^        if: matrix.runner$') 'Every test analyzer lane must run its full ordinary suite.'
    $buildPosition = $test.IndexOf('      - name: Build', [StringComparison]::Ordinal)
    $testPosition = $test.IndexOf('      - name: Test', [StringComparison]::Ordinal)
    Assert-Contract ($buildPosition -ge 0 -and $buildPosition -lt $testPosition) 'Ordinary tests must follow their successful Release analyzer build.'
    $afterBuild = $test.Substring($buildPosition + '      - name: Build'.Length)
    Assert-Contract ([regex]::Matches($afterBuild, '(?m)^[ \t]+(?:run:.*\b)?dotnet[ \t]+(?:build|restore|publish)\b').Count -eq 1) 'Ordinary tests must execute the validated binaries without rebuilding.'
    Assert-Contract ($test.Contains("dotnet-version: `${{ matrix.tf == 'net8.0' && '8.0.x' || '' }}")) 'The net8.0 analyzer lane must install its test runtime.'
    $testCommand = [regex]::Match($testStep, '(?m)^        run: (?<command>[^\n]+)$').Groups['command'].Value
    if ($testCommand -eq '>-') {
        $testCommand = ([regex]::Matches($testStep, '(?m)^          (?<line>[^\n]+)$') | ForEach-Object { $_.Groups['line'].Value.Trim() }) -join ' '
    }
    Assert-Contract ($testCommand.Contains('--test-modules JetDatabaseWriter.Tests.dll') -and $testCommand.Contains('--root-directory JetDatabaseWriter.Tests/bin/Release/${{ matrix.tf }}') -and $testCommand -match '(?:^| )--minimum-expected-tests 1(?: |$)') 'Ordinary tests must launch the validated module and reject empty discovery.'
    Assert-Contract ($testCommand -notmatch '(?:^| )--(?:filter|list-tests|explicit|project|solution)\b') 'Ordinary test jobs must run the full ordinary suite from the downloaded module.'
    Assert-Contract ($testStep.Contains('JDW_GOLDEN_OUTPUT_DIRECTORY: ${{ github.workspace }}/artifacts/golden/${{ matrix.tf }}')) 'Ordinary tests must retain normalized golden specimens in their framework lane.'
    $golden = [regex]::Match($analyze, '(?ms)^      - name: Upload golden diagnostics\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($golden.Contains('if: ${{ !cancelled() && matrix.runner }}') -and $golden.Contains('name: golden-bytes-${{ matrix.tf }}') -and $golden.Contains('path: artifacts/golden/${{ matrix.tf }}/')) 'Golden diagnostics must be uploaded after failing tests as well as successful tests.'
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
$net8Lane = '(?ms)^          - name: tests net8\.0\n.*?(?=^          - name:|^    steps:|\z)'
Assert-Scenario 'missing net8 analyzer lane' ([regex]::Replace($workflow, $net8Lane, '')) 'Every test framework must have exactly one analyzer lane'
Assert-Scenario 'wrong analyzer project selection' ($workflow.Replace('-f net8.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests', '-f net8.0 -p:AnalyzeOnly=JetDatabaseWriter.Test')) "The 'net8.0' test framework must have an analyzer lane"
Assert-Scenario 'incomplete solution analysis' ($workflow.Replace('project: JetDatabaseWriter.slnx', 'project: JetDatabaseWriter.TestSupport')) 'The tools lane must analyze the complete solution'
Assert-Scenario 'Debug analyzer build' ($workflow.Replace('dotnet build ${{ matrix.project }} -c Release', 'dotnet build ${{ matrix.project }} -c Debug')) 'The analyzer build must use Release'
Assert-Scenario 'disabled analyzers' ($workflow.Replace('dotnet build ${{ matrix.project }}', 'dotnet build -p:RunAnalyzers=false ${{ matrix.project }}')) 'Analyzer jobs must retain all strict build guarantees'
Assert-Scenario 'mismatched analyzer runner framework' ($workflow.Replace("tf: net8.0`n", "tf: net10.0`n")) "The 'net8.0' analyzer lane must publish its matching test runner"
Assert-Scenario 'ordinary suite disabled' ($workflow.Replace("      - name: Test`n        if: matrix.runner", "      - name: Test`n        if: false")) 'Every test analyzer lane must run its full ordinary suite'
Assert-Scenario 'unrelated analyzer barrier' ($workflow.Replace("    name: Analyze", "    needs: another-analyzer`n    name: Analyze")) 'Test analyzer lanes must start independently'
Assert-Scenario 'missing net8 runtime' ($workflow.Replace("          dotnet-version: `${{ matrix.tf == 'net8.0' && '8.0.x' || '' }}`n", '')) 'The net8.0 analyzer lane must install its test runtime'

$analyzeJob = Get-CiJob $workflow 'analyze'
$testStep = [regex]::Match($analyzeJob, '(?ms)^      - name: Test\n.*?(?=^      - |\z)').Value
Assert-Scenario 'missing ordinary suite' ($workflow.Replace($testStep, '')) 'Every test analyzer lane must execute the ordinary suite exactly once'
Assert-Scenario 'duplicate ordinary suite' ($workflow.Replace($testStep, $testStep + $testStep)) 'Every test analyzer lane must execute the ordinary suite exactly once'
Assert-Scenario 'tests before validation' ($workflow.Replace($testStep, '').Replace('      - name: Build', $testStep + '      - name: Build')) 'Ordinary tests must follow their successful Release analyzer build'
Assert-Scenario 'redundant ordinary test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: dotnet build JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the validated binaries without rebuilding'
Assert-Scenario 'redundant multiline test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: >-`n          dotnet build JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the validated binaries without rebuilding'
Assert-Scenario 'empty test discovery accepted' ($workflow.Replace('--minimum-expected-tests 1', '--minimum-expected-tests 0')) 'Ordinary tests must launch the validated module and reject empty discovery'
Assert-Scenario 'partial ordinary test selection' ($workflow.Replace('--minimum-expected-tests 1', '--minimum-expected-tests 1 --filter-class Example')) 'Ordinary test jobs must run the full ordinary suite'
Assert-Scenario 'golden evidence disabled' ($workflow.Replace('JDW_GOLDEN_OUTPUT_DIRECTORY:', 'REMOVED_GOLDEN_OUTPUT_DIRECTORY:')) 'Ordinary tests must retain normalized golden specimens'
Assert-Scenario 'golden evidence only on success' ($workflow.Replace('if: ${{ !cancelled() && matrix.runner }}', 'if: matrix.runner')) 'Golden diagnostics must be uploaded after failing tests'

$weakenedProps = [xml]$buildProps.OuterXml
$weakenedProps.SelectSingleNode('/Project/PropertyGroup/TreatWarningsAsErrors').InnerText = 'false'
Assert-Scenario 'warnings cease to be errors' $workflow "The 'TreatWarningsAsErrors' build guarantee must remain enabled" $weakenedProps
$additionalFramework = [xml]$testProject.OuterXml
$additionalFramework.SelectSingleNode('/Project/PropertyGroup/TargetFrameworks').InnerText += ';net9.0'
Assert-Scenario 'new test framework lacks hosted coverage' $workflow 'Every test framework must have exactly one analyzer lane' -Tests $additionalFramework
$suppressedWarning = [xml]$testProject.OuterXml
$suppressedWarning.SelectSingleNode('/Project/PropertyGroup/NoWarn').InnerText += ';CA2000'
Assert-Scenario 'blanket suppression hides disposal analysis' $workflow 'The test project must not add blanket warning suppressions' -Tests $suppressedWarning
