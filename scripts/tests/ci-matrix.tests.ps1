# Keep compilation distinct from runtime partitions and bind every consumer to its analyzed asset.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repository 'scripts/ci-matrix.ps1')

function Assert-Matrix($Analyzers, $Tests) {
    $builds = @($Analyzers.include)
    $runs = @($Tests.include)
    if ($builds.Count -ne 4) { throw 'Compile and analyze exactly the library, tools and two test assets.' }
    if ($runs.Count -ne 15) { throw 'Execute every partition on all three runtime/library combinations.' }
    $runnerBuilds = @($builds | Where-Object runner -EQ $true)
    if ($runnerBuilds.Count -ne 2) { throw 'Only the native and standard test assets publish runners.' }
    $artifacts = @{}
    foreach ($build in $runnerBuilds) {
        if ($build.asset -notin 'native', 'standard' -or $artifacts.ContainsKey($build.asset)) { throw 'Every runner asset must have exactly one analyzed producer.' }
        $artifacts[$build.asset] = $build
    }
    foreach ($runtime in 'net10.0', 'net10.0-netstandard', 'net11.0') {
        $expectedAsset = if ($runtime -eq 'net10.0-netstandard') { 'standard' } else { 'native' }
        foreach ($partition in 'ordinary', 'schema-writer', 'jet3', 'jet4', 'ace') {
            $run = @($runs | Where-Object { $_.tf -ceq $runtime -and $_.partition -ceq $partition })
            if ($run.Count -ne 1 -or $run[0].asset -cne $expectedAsset -or -not $artifacts.ContainsKey($run[0].asset)) { throw 'Every runtime partition must consume its exact analyzed asset once.' }
            if ($run[0].Contains('project') -or $run[0].Contains('options') -or $run[0].Contains('restore-options')) { throw 'Test consumers must not compile or restore.' }
        }
    }
    $standard = $artifacts.standard
    if ($standard['restore-options'] -cne '-p:TestNetStandard=true' -or $standard.options -cne '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests -p:TestNetStandard=true') { throw 'The standard runner must build and restore the standard-library dependency graph.' }
    if ($artifacts.native.options -cne '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests' -or $artifacts.native['restore-options']) { throw 'The native runner must be analyzed once with the pinned SDK.' }
}

$analyzers = Get-CiAnalyzerMatrix
$tests = Get-CiTestMatrix
Assert-Matrix $analyzers $tests
'PASS: four analyzed builds feed all fifteen runtime partitions'
foreach ($case in @(
    @{ name = 'wrong library asset'; expected = 'consume its exact analyzed asset'; mutate = { param($a, $t) $t.include[0].asset = 'standard' } }
    @{ name = 'missing runtime partition'; expected = 'all three runtime/library'; mutate = { param($a, $t) $t.include = @($t.include | Select-Object -Skip 1) } }
    @{ name = 'duplicate runtime partition'; expected = 'consume its exact analyzed asset'; mutate = { param($a, $t) $t.include[1] = $t.include[0] } }
    @{ name = 'consumer repeats build'; expected = 'must not compile or restore'; mutate = { param($a, $t) $t.include[0].project = 'tests/JetDatabaseWriter.Tests' } }
    @{ name = 'duplicate asset producer'; expected = 'exactly one analyzed producer'; mutate = { param($a, $t) $a.include[3].asset = 'native' } }
    @{ name = 'standard graph not restored'; expected = 'standard-library dependency graph'; mutate = { param($a, $t) $a.include[3]['restore-options'] = '' } }
)) {
    $candidateAnalyzers = $analyzers | ConvertTo-Json -Depth 5 | ConvertFrom-Json -AsHashtable
    $candidateTests = $tests | ConvertTo-Json -Depth 5 | ConvertFrom-Json -AsHashtable
    & $case.mutate $candidateAnalyzers $candidateTests
    $caught = $null
    try { Assert-Matrix $candidateAnalyzers $candidateTests } catch { $caught = $_.Exception.Message }
    if (-not $caught -or -not $caught.Contains($case.expected)) { throw "Matrix accepted $($case.name): $caught" }
    "PASS: refused $($case.name)"
}
