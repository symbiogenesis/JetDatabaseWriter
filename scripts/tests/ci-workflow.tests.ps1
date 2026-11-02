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
$matrix = @((Get-CiAnalyzerMatrix).include) + @((Get-CiTestMatrix).include)

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
    $events = [regex]::Match($Workflow, '(?ms)^on:\n(?<events>.*?)^permissions:\n').Groups['events'].Value
    Assert-Contract ($events -match '(?m)^  push:$' -and $events -match '(?m)^      - main$' -and $events -match '(?m)^  pull_request:$' -and $events -match '(?m)^  workflow_dispatch:$' -and $events -notmatch '(?m)^    paths(?:-ignore)?:') 'Push and PR triggers must retain the required gate for every content type; select jobs inside the workflow.'
    Assert-Contract ($Workflow.Contains('group: ci-${{ github.ref }}') -and $Workflow.Contains("cancel-in-progress: `${{ !startsWith(github.ref, 'refs/tags/') }}")) 'Superseded branch runs must be cancelled without cancelling release runs.'
    $analyze = Get-CiJob $Workflow 'analyze'
    $test = Get-CiJob $Workflow 'test'
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
    Assert-Contract (($frameworks -join ';') -eq 'net10.0') 'Tests must build locally with the pinned .NET 10 SDK.'
    $buildLanes = @($lanes | Where-Object project -eq 'tests/JetDatabaseWriter.Tests')
    Assert-Contract ($buildLanes.Count -eq 2) 'Exactly two analyzed test assets must serve every runtime partition.'
    foreach ($asset in 'native', 'standard') {
        $buildLane = @($buildLanes | Where-Object asset -CEQ $asset)
        $expectedOptions = '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests'
        $restoreOptions = ''
        if ($asset -eq 'standard') { $expectedOptions += ' -p:TestNetStandard=true'; $restoreOptions = '-p:TestNetStandard=true' }
        Assert-Contract ($buildLane.Count -eq 1 -and $buildLane[0].name -ceq "tests $asset" -and $buildLane[0].options -ceq $expectedOptions -and [string]$buildLane[0]['restore-options'] -ceq $restoreOptions -and $buildLane[0].runner -is [bool] -and $buildLane[0].runner) 'The two test assets must restore and analyze their exact dependency graphs once.'
    }
    $testLanes = @($lanes | Where-Object partition)
    Assert-Contract ($testLanes.Count -eq 15) 'Native .NET 10, netstandard and .NET 11 must each have five test partitions.'
    $sweepClasses = [ordered]@{
        jet3 = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityJet3Tests'
        jet4 = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityJet4Tests'
        ace = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityAceTests'
    }
    $partitionNamespaces = 'JetDatabaseWriter.Tests.Schema JetDatabaseWriter.Tests.RoundTrip JetDatabaseWriter.Tests.Writer'
    $partitions = @('ordinary', 'schema-writer') + @($sweepClasses.Keys)
    foreach ($framework in 'net10.0', 'net10.0-netstandard', 'net11.0') {
        $expectedAsset = if ($framework -eq 'net10.0-netstandard') { 'standard' } else { 'native' }
        foreach ($partition in $partitions) {
            $lane = @($testLanes | Where-Object { $_.tf -eq $framework -and $_.partition -eq $partition })
            Assert-Contract ($lane.Count -eq 1 -and $lane[0].asset -ceq $expectedAsset -and -not $lane[0].project -and -not $lane[0].options -and -not $lane[0]['restore-options']) "The '$framework/$partition' test partition must execute its matching analyzed asset exactly once."
            $name = 'tests ' + $framework.Replace('net10.0-netstandard', 'net10.0 netstandard') + ' ' + $partition
            Assert-Contract ($lane[0].name -ceq $name) 'Test partition job names must match the validated ancestor job inventory.'
            $expectedFilter = switch ($partition) {
                ordinary { '--filter-not-namespace ' + $partitionNamespaces }
                schema-writer { '--filter-namespace ' + $partitionNamespaces + ' --filter-not-class ' + ($sweepClasses.Values -join ' ') }
                default { '--filter-class ' + $sweepClasses[$partition] }
            }
            Assert-Contract ($lane[0]['test-options'] -ceq $expectedFilter) "The '$framework/$partition' filter must retain the exhaustive, disjoint partition of the full ordinary suite."
        }
    }
    Assert-Contract ($lanes.Count -eq 19) 'Unexpected build or execution lanes require a coverage review.'
    Assert-Contract (@($lanes.name | Sort-Object -Unique).Count -eq $lanes.Count) 'Every build and execution partition must have a unique job name.'

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

    $upload = [regex]::Match($analyze, '(?ms)^      - name: Upload analyzed test runner\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($upload.Contains('if: matrix.runner') -and $upload.Contains('name: analyzed-runner-${{ matrix.asset }}') -and $upload.Contains('path: tests/JetDatabaseWriter.Tests/bin/Release/net10.0/') -and $upload -match 'uses: actions/upload-artifact@[0-9a-f]{40}(?:\s|$)' -and $upload.Contains('if-no-files-found: error')) 'Each analyzed test asset must publish its complete runner exactly once with a pinned action.'
    Assert-Contract ($analyze.IndexOf('      - name: Build', [StringComparison]::Ordinal) -lt $analyze.IndexOf('      - name: Upload analyzed test runner', [StringComparison]::Ordinal) -and $upload -notmatch '(?m)^        if:.*(?:always|cancelled)') 'Analyzed runners may publish only after their successful Release build.'
    foreach ($name in 'analyze', 'aot', 'encryption-replacement') {
        $laneJob = Get-CiJob $Workflow $name
        Assert-Contract ($laneJob -match '(?m)^    needs: changes$' -and [regex]::Matches($laneJob, '(?m)^    needs:').Count -eq 1) 'Analyzer lanes must start independently of other analyzer lanes.'
        Assert-Contract ($laneJob -match "(?m)^    if: needs\.changes\.outputs\.dotnet == 'true'$" -and [regex]::Matches($laneJob, '(?m)^    if:').Count -eq 1) 'All .NET jobs must use the same reviewed lane selection.'
    }
    Assert-Contract ($test.Contains('needs: [changes, analyze]') -and $test -match "(?m)^    if: needs\.changes\.outputs\.dotnet == 'true'$" -and $test.Contains('name: Test (${{ matrix.name }})') -and $analyze.Contains('name: Analyze (${{ matrix.name }})')) 'Test consumers must wait for successful analyzed artifacts and match the ancestor inventory.'
    Assert-Contract ($test.Contains('matrix: ${{ fromJSON(needs.changes.outputs.test-matrix) }}')) 'The test matrix must consume every shared runtime partition.'
    $download = [regex]::Match($test, '(?ms)^      - name: Download analyzed test runner\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($download.Contains('name: analyzed-runner-${{ matrix.asset }}') -and $download.Contains('path: artifacts/test-runner') -and $download -match 'uses: actions/download-artifact@[0-9a-f]{40}(?:\s|$)') 'Each test partition must download its exact analyzed asset with a pinned action.'
    $testSteps = [regex]::Matches($test, '(?ms)^      - name: Test\n(?<step>.*?)(?=^      - |\z)')
    Assert-Contract ($testSteps.Count -eq 1) 'Every test partition must execute its assigned tests exactly once.'
    $testStep = $testSteps[0].Groups['step'].Value
    Assert-Contract ($testStep -notmatch '(?m)^        if:') 'Every test partition must run its assigned tests.'
    $downloadPosition = $test.IndexOf('      - name: Download analyzed test runner', [StringComparison]::Ordinal)
    $testPosition = $test.IndexOf('      - name: Test', [StringComparison]::Ordinal)
    Assert-Contract ($downloadPosition -ge 0 -and $downloadPosition -lt $testPosition) 'Ordinary tests must follow acquisition of their analyzed runner.'
    Assert-Contract ($test -notmatch '\bdotnet\s+(?:build|restore|publish|pack)\b') 'Ordinary tests must execute the analyzed binaries without rebuilding.'
    Assert-Contract ($test.Contains('global-json-file: .github/dotnet11/global.json') -and $test.Contains("if: matrix.tf == 'net11.0'")) 'The .NET 11 lane must install the pinned prerelease runtime.'
    Assert-Contract ($testStep.Contains("DOTNET_ROLL_FORWARD: `${{ matrix.tf == 'net11.0' && 'LatestMajor' || 'LatestPatch' }}") -and $testStep.Contains("DOTNET_ROLL_FORWARD_TO_PRERELEASE: `${{ matrix.tf == 'net11.0' && '1' || '0' }}") -and $testStep.Contains("JDW_EXPECTED_RUNTIME_MAJOR: `${{ matrix.tf == 'net11.0' && '11' || '10' }}")) 'Test lanes must select and verify their intended runtime, including prereleases.'
    $testCommand = [regex]::Match($testStep, '(?m)^        run: (?<command>[^\n]+)$').Groups['command'].Value
    if ($testCommand -eq '>-') {
        $testCommand = ([regex]::Matches($testStep, '(?m)^          (?<line>[^\n]+)$') | ForEach-Object { $_.Groups['line'].Value.Trim() }) -join ' '
    }
    Assert-Contract ($testCommand.Contains('--test-modules JetDatabaseWriter.Tests.dll') -and $testCommand.Contains('--root-directory artifacts/test-runner') -and $testCommand -match '(?:^| )--minimum-expected-tests 48(?: |$)') 'Every partition must launch the validated module and require at least all 48 fault theory rows.'
    Assert-Contract ($testCommand.Contains('${{ matrix.test-options }}') -and $testCommand -notmatch '(?:^| )--(?:filter|list-tests|explicit|project|solution)\b') 'Test partitions must use only their reviewed exhaustive matrix filters.'
    Assert-Contract ($testCommand.Contains('--results-directory artifacts/test-results/${{ matrix.tf }}-${{ matrix.partition }}')) 'Test result directories must distinguish every framework and partition.'
    Assert-Contract ($testStep.Contains('JDW_GOLDEN_OUTPUT_DIRECTORY: ${{ github.workspace }}/artifacts/golden/${{ matrix.tf }}-${{ matrix.partition }}')) 'Ordinary tests must retain normalized golden specimens in their framework partition.'
    $results = [regex]::Match($test, '(?ms)^      - name: Upload test results\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($results.Contains('if: ${{ !cancelled() }}') -and $results.Contains('name: test-results-${{ matrix.tf }}-${{ matrix.partition }}') -and $results.Contains('path: artifacts/test-results/${{ matrix.tf }}-${{ matrix.partition }}/')) 'Every partition must retain uniquely named test results after failures as well as successes.'
    $skipPolicy = [regex]::Match($test, '(?ms)^      - name: Validate test outcomes\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($skipPolicy.Contains('if: ${{ !cancelled() }}') -and $skipPolicy.Contains('shell: pwsh') -and $skipPolicy.Contains('./scripts/assert-test-results.ps1 -ResultsDirectory artifacts/test-results/${{ matrix.tf }}-${{ matrix.partition }}')) 'Every ordinary partition must reject unapproved skips and missing or failed xUnit results.'
    $golden = [regex]::Match($test, '(?ms)^      - name: Upload golden diagnostics\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($golden.Contains('if: ${{ !cancelled() }}') -and $golden.Contains('name: golden-bytes-${{ matrix.tf }}-${{ matrix.partition }}') -and $golden.Contains('path: artifacts/golden/${{ matrix.tf }}-${{ matrix.partition }}/')) 'Every partition must retain uniquely named golden diagnostics after failures as well as successes.'
    Assert-Contract ($Workflow -notmatch '(?i)infersharp') 'InferSharp must remain in its independent workflow.'

    $changes = Get-CiJob $Workflow 'changes'
    Assert-Contract ($changes.Contains('matrix: ${{ steps.select.outputs.matrix }}') -and $changes.Contains('test-matrix: ${{ steps.select.outputs.test-matrix }}')) 'The selection job must expose both complete shared matrices.'
    Assert-Contract ($changes.Contains('package: ${{ steps.select.outputs.package }}')) 'The selection job must expose documentation package selection.'
    Assert-Contract ($changes.Contains('fetch-depth: 0') -and $changes.Contains('run: ./scripts/select-ci-lanes.ps1') -and $changes.Contains('GITHUB_TOKEN: ${{ github.token }}') -and $changes -notmatch '(?m)^    (?:if|needs):') 'Path selection must inspect complete history and run unconditionally.'
    Assert-Contract ($Workflow.Contains("  workflow_call:`n    inputs:`n      full:") -and $Workflow.Contains('        default: true') -and $changes.Contains('JDW_CI_FORCE_FULL: ${{ inputs.full || false }}')) 'Reusable workflow calls must default to complete validation.'
    $checks = Get-CiJob $Workflow 'checks'
    Assert-Contract ($checks -notmatch '(?m)^    (?:if|needs):' -and $checks.Contains('run: ./scripts/test-scripts.ps1') -and $checks.Contains('run: ./scripts/check-security-traceability.ps1')) 'Script and security checks must remain unconditional.'
    $package = Get-CiJob $Workflow 'package'
    foreach ($packedJob in @(@{ Body = $analyze; Conditional = $true }, @{ Body = $package; Conditional = $false })) {
        $consumer = [regex]::Match($packedJob.Body, '(?ms)^      - name: Test packed consumers\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
        $condition = if ($packedJob.Conditional) { $consumer -match '(?m)^        if: matrix\.pack$' } else { $consumer -notmatch '(?m)^        if:' }
        Assert-Contract ($consumer.Contains('run: ./scripts/test-package.ps1 -PackageDirectory artifacts/nupkg') -and $consumer.Contains('shell: pwsh') -and $condition -and $consumer -notmatch 'continue-on-error:') 'Every produced package must pass its actual package consumers before upload.'
        $packPosition = $packedJob.Body.IndexOf('      - name: Pack', [StringComparison]::Ordinal)
        $consumerPosition = $packedJob.Body.IndexOf('      - name: Test packed consumers', [StringComparison]::Ordinal)
        $uploadPosition = $packedJob.Body.IndexOf('      - name: Upload package', [StringComparison]::Ordinal)
        Assert-Contract ($packPosition -ge 0 -and $packPosition -lt $consumerPosition -and $consumerPosition -lt $uploadPosition) 'Package consumption must follow packing and precede artifact publication.'
    }
    Assert-Contract ($package -match '(?m)^    needs: changes$' -and $package -match "(?m)^    if: needs\.changes\.outputs\.package == 'true'$" -and $package.Contains('name: Package documentation')) 'Packed documentation must use the focused package selection.'
    Assert-Contract ($package.Contains('fetch-depth: 0') -and $package.Contains('global-json-file: global.json') -and $package.Contains('dotnet restore src/JetDatabaseWriter/JetDatabaseWriter.csproj --locked-mode') -and $package.Contains('dotnet pack src/JetDatabaseWriter/JetDatabaseWriter.csproj -c Release --no-restore -o artifacts/nupkg') -and $package -notmatch 'dotnet test|--no-build|continue-on-error:') 'Documentation packaging must restore locked dependencies and build the strict Release library without running the test suite.'
    Assert-Contract ($package.Contains('name: nupkg') -and $package.Contains('path: artifacts/nupkg/*.nupkg') -and $package.Contains('if-no-files-found: error')) 'Documentation packaging must retain its package artifact.'
    $gate = Get-CiJob $Workflow 'gate'
    Assert-Contract ($gate.Contains('needs: [changes, checks, formal, analyze, test, native-access, aot, encryption-replacement, package]') -and $gate.Contains('if: ${{ !cancelled() }}') -and $gate.Contains('JDW_CI_NEEDS: ${{ toJSON(needs) }}') -and $gate.Contains('run: ./scripts/select-ci-lanes.ps1 -CheckResults')) 'The aggregate gate must reject failure and missing selected jobs while honoring workflow cancellation.'
    $native = Get-CiJob $Workflow 'native-access'
    Assert-Contract ($native.Contains('name: Native Access compatibility (${{ matrix.tf }})') -and $native.Contains('needs: [changes, analyze]') -and $native.Contains('if: ${{ !cancelled() && needs.changes.outputs.dotnet == ''true'' }}') -and $native.Contains('runs-on: windows-latest') -and $native.Contains('timeout-minutes: 90') -and $native -notmatch 'continue-on-error:') 'Native compatibility must gate every selected .NET revision after its analyzed artifacts are available, including independent evidence after self-test failures.'
    Assert-Contract ($native.Contains('tf: [net10.0, net10.0-netstandard, net11.0]') -and $native.Contains('global-json-file: global.json') -and $native.Contains('global-json-file: .github/dotnet11/global.json')) 'Native compatibility must cover all three runtime/library combinations.'
    Assert-Contract ($native -match 'uses: actions/download-artifact@[0-9a-f]{40}(?:\s|$)' -and $native.Contains('name: analyzed-runner-${{ matrix.tf == ''net10.0-netstandard'' && ''standard'' || ''native'' }}') -and $native.Contains('path: artifacts/native-runner') -and $native -notmatch 'dotnet (?:build|restore|publish)\b') 'Native compatibility must consume the exact analyzed runner without rebuilding.'
    Assert-Contract ($native.Contains('run: ./scripts/install-native-access.ps1') -and $native.Contains('run: ./scripts/run-native-compatibility.ps1 -RunnerDirectory artifacts/native-runner -ResultsDirectory artifacts/native-results/${{ matrix.tf }}') -and $native.IndexOf('run: ./scripts/install-native-access.ps1', [StringComparison]::Ordinal) -lt $native.IndexOf('run: ./scripts/run-native-compatibility.ps1', [StringComparison]::Ordinal)) 'Native compatibility must provision the pinned independent engine before executing its zero-skip method inventory.'
    Assert-Contract ($native.Contains("JDW_EXPECTED_RUNTIME_MAJOR: `${{ matrix.tf == 'net11.0' && '11' || '10' }}") -and $native.Contains("DOTNET_ROLL_FORWARD: `${{ matrix.tf == 'net11.0' && 'LatestMajor' || 'LatestPatch' }}") -and $native.Contains("DOTNET_ROLL_FORWARD_TO_PRERELEASE: `${{ matrix.tf == 'net11.0' && '1' || '0' }}")) 'Native compatibility must execute its declared runtime, including prereleases.'
    $nativeResults = [regex]::Match($native, '(?ms)^      - name: Upload native results\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($nativeResults.Contains('if: ${{ !cancelled() }}') -and $nativeResults.Contains('name: native-results-${{ matrix.tf }}') -and $nativeResults.Contains('path: artifacts/native-results/${{ matrix.tf }}/')) 'Native result evidence must survive test or engine-provisioning failures.'
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
Assert-Scenario 'docs hidden from required push gate' ($workflow.Replace("  push:`n", "  push:`n    paths-ignore:`n      - '**.md'`n")) 'Push and PR triggers must retain the required gate'
Assert-Scenario 'PR content filter suppresses required gate' ($workflow.Replace("  pull_request:`n", "  pull_request:`n    paths:`n      - 'src/**'`n")) 'Push and PR triggers must retain the required gate'
Assert-Scenario 'package selection omitted' ($workflow.Replace('package: ${{ steps.select.outputs.package }}', 'removed: value')) 'The selection job must expose documentation package selection'
Assert-Scenario 'documentation packaging disabled' ($workflow.Replace("if: needs.changes.outputs.package == 'true'", 'if: false')) 'Packed documentation must use the focused package selection'
Assert-Scenario 'packed consumer validation omitted' ($workflow.Replace('./scripts/test-package.ps1', './scripts/removed-package-check.ps1')) 'Every produced package must pass its actual package consumers before upload'
Assert-Scenario 'packed consumers disabled' ($workflow.Replace("      - name: Test packed consumers`n        if: matrix.pack", "      - name: Test packed consumers`n        if: false")) 'Every produced package must pass its actual package consumers before upload'
Assert-Scenario 'failed package consumers accepted' ($workflow.Replace("      - name: Test packed consumers`n", "      - name: Test packed consumers`n        continue-on-error: true`n")) 'Analyzer jobs must retain all strict build guarantees'
Assert-Scenario 'documentation package reuses missing binaries' ($workflow.Replace('-c Release --no-restore -o artifacts/nupkg', '-c Release --no-build -o artifacts/nupkg')) 'Documentation packaging must restore locked dependencies'
Assert-Scenario 'documentation package omitted from gate' ($workflow.Replace('encryption-replacement, package]', 'encryption-replacement]')) 'The aggregate gate must reject failure'
Assert-Scenario 'obsolete branch runs block current validation' ($workflow.Replace("cancel-in-progress: `${{ !startsWith(github.ref, 'refs/tags/') }}", 'cancel-in-progress: false')) 'Superseded branch runs must be cancelled'
Assert-MatrixScenario 'incomplete solution analysis' { param($lanes) $lanes[1].project = 'tests/JetDatabaseWriter.TestSupport'; $lanes } 'The tools lane must analyze the complete tools filter'
Assert-Scenario 'shared matrix ignored' ($workflow.Replace('matrix: ${{ fromJSON(needs.changes.outputs.matrix) }}', 'matrix: {}')) 'The analyzer matrix must consume the complete shared definition'
Assert-Scenario 'shared matrix output omitted' ($workflow.Replace('matrix: ${{ steps.select.outputs.matrix }}', 'removed: value')) 'The selection job must expose both complete shared matrices'
Assert-Scenario 'test matrix output omitted' ($workflow.Replace('test-matrix: ${{ steps.select.outputs.test-matrix }}', 'removed: value')) 'The selection job must expose both complete shared matrices'
Assert-Scenario 'test matrix ignored' ($workflow.Replace('matrix: ${{ fromJSON(needs.changes.outputs.test-matrix) }}', 'matrix: {}')) 'The test matrix must consume every shared runtime partition'
Assert-Scenario 'Debug analyzer build' ($workflow.Replace('dotnet build ${{ matrix.project }} -c Release', 'dotnet build ${{ matrix.project }} -c Debug')) 'The analyzer build must use Release'
Assert-Scenario 'disabled analyzers' ($workflow.Replace('dotnet build ${{ matrix.project }}', 'dotnet build -p:RunAnalyzers=false ${{ matrix.project }}')) 'Analyzer jobs must retain all strict build guarantees'
Assert-Scenario 'ordinary suite disabled' ($workflow.Replace("      - name: Test`n", "      - name: Test`n        if: false`n")) 'Every test partition must run its assigned tests'
Assert-Scenario 'skip policy omitted' ($workflow.Replace('      - name: Validate test outcomes', '      - name: Removed test outcomes')) 'Every ordinary partition must reject unapproved skips'
Assert-Scenario 'native compatibility omitted from gate' ($workflow.Replace('test, native-access, aot', 'test, aot')) 'The aggregate gate must reject failure'
Assert-Scenario 'test execution omitted from gate' ($workflow.Replace('analyze, test, native-access', 'analyze, native-access')) 'The aggregate gate must reject failure'
Assert-Scenario 'native evidence blocked by self-test failures' ($workflow.Replace('if: ${{ !cancelled() && needs.changes.outputs.dotnet == ''true'' }}', 'if: needs.changes.outputs.dotnet == ''true''')) 'including independent evidence after self-test failures'
Assert-Scenario 'native runner rebuilt' ($workflow.Replace('run: ./scripts/run-native-compatibility.ps1', 'run: dotnet build; ./scripts/run-native-compatibility.ps1')) 'Native compatibility must consume the exact analyzed runner'
Assert-Scenario 'native engine not provisioned' ($workflow.Replace('run: ./scripts/install-native-access.ps1', 'run: echo already-installed')) 'Native compatibility must provision the pinned independent engine'
Assert-Scenario 'native target omitted' ($workflow.Replace('tf: [net10.0, net10.0-netstandard, net11.0]', 'tf: [net10.0, net11.0]')) 'Native compatibility must cover all three runtime/library combinations'
Assert-Scenario 'unrelated analyzer barrier' ($workflow.Replace("    name: Analyze", "    needs: another-analyzer`n    name: Analyze")) 'Analyzer lanes must start independently'
Assert-Scenario 'dotnet lane selection disabled' ($workflow.Replace("if: needs.changes.outputs.dotnet == 'true'", 'if: false')) 'All .NET jobs must use the same reviewed lane selection'
Assert-Scenario 'missing aggregate gate' ($workflow.Replace('  gate:', '  removed:')) "The 'gate' CI job is missing"
$gateJob = Get-CiJob $workflow 'gate'
Assert-Scenario 'aggregate ignores skipped prerequisites' ($workflow.Replace($gateJob, $gateJob.Replace('if: ${{ !cancelled() }}', 'if: success()'))) 'The aggregate gate must reject failure'
Assert-Scenario 'aggregate blocks superseding runs after cancellation' ($workflow.Replace($gateJob, $gateJob.Replace('if: ${{ !cancelled() }}', 'if: always()'))) 'honoring workflow cancellation'

$testJob = Get-CiJob $workflow 'test'
$testStep = [regex]::Match($testJob, '(?ms)^      - name: Test\n.*?(?=^      - |\z)').Value
Assert-Scenario 'test runner download binding omitted' ($workflow.Replace('path: artifacts/test-runner', 'path: artifacts/unbound-runner')) 'Each test partition must download its exact analyzed asset'
Assert-Scenario 'test consumer ignores analyzed build failure' ($workflow.Replace("  test:`n    needs: [changes, analyze]", "  test:`n    needs: changes")) 'Test consumers must wait for successful analyzed artifacts'
$analyzerJob = Get-CiJob $workflow 'analyze'
$uploadStep = [regex]::Match($analyzerJob, '(?ms)^      - name: Upload analyzed test runner\n.*?(?=^      - |\z)').Value
Assert-Scenario 'runner uploaded before analysis' ($workflow.Replace($uploadStep, '').Replace('      - name: Build', $uploadStep + '      - name: Build')) 'Analyzed runners may publish only after their successful Release build'
Assert-Scenario 'failed analysis publishes runner' ($workflow.Replace('if: matrix.runner', 'if: always() && matrix.runner')) 'Each analyzed test asset must publish its complete runner exactly once'
Assert-Scenario 'missing ordinary suite' ($workflow.Replace($testStep, '')) 'Every test partition must execute its assigned tests exactly once'
Assert-Scenario 'duplicate ordinary suite' ($workflow.Replace($testStep, $testStep + $testStep)) 'Every test partition must execute its assigned tests exactly once'
Assert-Scenario 'tests before validation' ($workflow.Replace($testStep, '').Replace('      - name: Download analyzed test runner', $testStep + '      - name: Download analyzed test runner')) 'Ordinary tests must follow acquisition of their analyzed runner'
Assert-Scenario 'redundant ordinary test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: dotnet build tests/JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the analyzed binaries without rebuilding'
Assert-Scenario 'redundant multiline test build' ($workflow.Replace($testStep, "      - name: Rebuild`n        run: >-`n          dotnet build tests/JetDatabaseWriter.Tests -c Release`n" + $testStep)) 'Ordinary tests must execute the analyzed binaries without rebuilding'
Assert-Scenario 'empty test discovery accepted' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 0')) 'Every partition must launch the validated module and require at least all 48 fault theory rows'
Assert-Scenario 'incomplete fault discovery accepted' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 47')) 'Every partition must launch the validated module and require at least all 48 fault theory rows'
Assert-Scenario 'partial ordinary test selection' ($workflow.Replace('--minimum-expected-tests 48', '--minimum-expected-tests 48 --filter-class Example')) 'Test partitions must use only their reviewed exhaustive matrix filters'
Assert-Scenario 'partition filters unused' ($workflow.Replace('${{ matrix.test-options }}', '')) 'Test partitions must use only their reviewed exhaustive matrix filters'
Assert-MatrixScenario 'unexpected ordinary exclusion' { param($lanes) $lanes[4]['test-options'] += ' Example'; $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'namespace gap omits schema cases' { param($lanes) $lanes[5]['test-options'] = $lanes[5]['test-options'].Replace('JetDatabaseWriter.Tests.Schema ', ''); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'namespace overlap repeats schema cases' { param($lanes) $lanes[4]['test-options'] = $lanes[4]['test-options'].Replace('JetDatabaseWriter.Tests.Schema ', ''); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'unreviewed namespace inclusion repeats ordinary cases' { param($lanes) $lanes[5]['test-options'] = $lanes[5]['test-options'].Replace('--filter-namespace ', '--filter-namespace JetDatabaseWriter.Tests.Encryption '); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'sweep partition selects wrong class' { param($lanes) $lanes[6]['test-options'] = $lanes[6]['test-options'].Replace('StatementAtomicityJet3Tests', 'StatementAtomicityJet4Tests'); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'sweep partition disables explicit cases' { param($lanes) $lanes[6]['test-options'] += ' --explicit off'; $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-MatrixScenario 'schema writer partition duplicates sweep cases' { param($lanes) $lanes[5]['test-options'] = $lanes[5]['test-options'].Replace('JetDatabaseWriter.Tests.Writer.StatementAtomicityJet3Tests ', ''); $lanes } 'filter must retain the exhaustive, disjoint partition'
Assert-Scenario 'framework partitions collide in result directory' ($workflow.Replace('--results-directory artifacts/test-results/${{ matrix.tf }}-${{ matrix.partition }}', '--results-directory artifacts/test-results/${{ matrix.tf }}')) 'Test result directories must distinguish every framework and partition'
Assert-Scenario 'partition results collide in artifact name' ($workflow.Replace('name: test-results-${{ matrix.tf }}-${{ matrix.partition }}', 'name: test-results-${{ matrix.tf }}')) 'Every partition must retain uniquely named test results'
Assert-Scenario 'runner artifact binding omitted' ($workflow.Replace('name: analyzed-runner-${{ matrix.asset }}', 'name: unbound-runner')) 'Each analyzed test asset must publish its complete runner exactly once'
Assert-Scenario 'golden evidence disabled' ($workflow.Replace('JDW_GOLDEN_OUTPUT_DIRECTORY:', 'REMOVED_GOLDEN_OUTPUT_DIRECTORY:')) 'Ordinary tests must retain normalized golden specimens'
Assert-Scenario 'test result evidence only on success' ($workflow.Replace("      - name: Upload test results`n        if: `${{ !cancelled() }}", "      - name: Upload test results`n        if: success()")) 'Every partition must retain uniquely named test results'
Assert-Scenario 'golden evidence only on success' ($workflow.Replace("      - name: Upload golden diagnostics`n        if: `${{ !cancelled() }}", "      - name: Upload golden diagnostics`n        if: success()")) 'Every partition must retain uniquely named golden diagnostics'
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

Assert-MatrixScenario 'missing standard asset partition' { param($lanes) $lanes | Where-Object name -CNE 'tests net10.0 netstandard ordinary' } 'Native .NET 10, netstandard and .NET 11 must each have five test partitions'
Assert-MatrixScenario 'duplicate standard asset partition' { param($lanes) $lanes; $lanes[9] } 'Native .NET 10, netstandard and .NET 11 must each have five test partitions'
Assert-MatrixScenario 'duplicate namespace partition misses ordinary tests' { param($lanes) $lanes[4].partition = 'schema-writer'; $lanes } "The 'net10.0/ordinary' test partition must execute"
Assert-MatrixScenario 'wrong standard analyzer project' { param($lanes) $lanes[3].options = '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Test -p:TestNetStandard=true'; $lanes } 'The two test assets must restore and analyze'
Assert-MatrixScenario 'mismatched runtime runner' { param($lanes) $lanes[14].tf = 'net10.0'; $lanes } "The 'net10.0/ordinary' test partition must execute"
Assert-MatrixScenario 'duplicate sweep format misses another partition' { param($lanes) $lanes[8].partition = 'jet4'; $lanes } "The 'net10.0/jet4' test partition must execute"
Assert-MatrixScenario 'unknown sweep partition omits a format' { param($lanes) $lanes[8].partition = 'other'; $lanes } "The 'net10.0/ace' test partition must execute"
Assert-MatrixScenario 'renamed job no longer matches ancestor inventory' { param($lanes) $lanes[4].name = 'tests net10.0 renamed'; $lanes } 'Test partition job names must match the validated ancestor job inventory'
Assert-MatrixScenario 'standard graph not restored' { param($lanes) $lanes[3]['restore-options'] = 'removed'; $lanes } 'The two test assets must restore and analyze'
Assert-MatrixScenario 'package creation omitted' { param($lanes) $lanes[0].pack = $false; $lanes } 'Library and tools job names and package creation must match'
Assert-MatrixScenario 'package condition is a truthy string' { param($lanes) $lanes[0].pack = 'true'; $lanes } 'Library and tools job names and package creation must match'
Assert-MatrixScenario 'runner condition is a truthy string' { param($lanes) $lanes[2].runner = 'true'; $lanes } 'The two test assets must restore and analyze'
Assert-Scenario 'missing prerelease installation' ($workflow.Replace('global-json-file: .github/dotnet11/global.json', 'global-json-file: global.json')) 'The .NET 11 lane must install'
Assert-Scenario 'runtime stays on installed 10' ($workflow.Replace("'LatestMajor'", "'Major'")) 'Test lanes must select and verify their intended runtime'
Assert-Scenario 'prerelease runtime excluded' ($workflow.Replace("DOTNET_ROLL_FORWARD_TO_PRERELEASE:", 'REMOVED_PRERELEASE:')) 'Test lanes must select and verify their intended runtime'
Assert-Scenario 'runtime assertion disabled' ($workflow.Replace('JDW_EXPECTED_RUNTIME_MAJOR:', 'REMOVED_RUNTIME_ASSERTION:')) 'Test lanes must select and verify their intended runtime'

Assert-Scenario 'InferSharp added to ordinary CI' ($workflow + '
# InferSharp') 'InferSharp must remain in its independent workflow.'
