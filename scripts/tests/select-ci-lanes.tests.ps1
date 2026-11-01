# Path selection must retain coverage across failed, pending and multi-commit pushes.
[CmdletBinding()]
param([string] $HelperPath)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$errors = $null
if (-not $HelperPath) { $HelperPath = Join-Path $repoRoot 'scripts/select-ci-lanes.ps1' }
. (Join-Path (Split-Path $HelperPath -Parent) 'ci-matrix.ps1')
$ast = [Management.Automation.Language.Parser]::ParseFile($HelperPath, [ref]$null, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors.Message -join '; ') }
foreach ($definition in $ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
    . ([scriptblock]::Create($definition.Extent.Text))
}

function Assert-Lanes([string] $Name, [string[]] $Paths, [bool] $Dotnet, [bool] $Formal, [bool] $Package = $false) {
    $actual = Get-CiLanes $Paths
    if ($actual.dotnet -ne $Dotnet -or $actual.formal -ne $Formal -or $actual.package -ne $Package) { throw "$Name selected incorrect lanes: $($actual | ConvertTo-Json -Compress)" }
    "PASS: $Name"
}
Assert-Lanes 'unchanged validated tree' @() $false $false
Assert-Lanes 'readme only needs package validation' @('README.md') $false $false $true
Assert-Lanes 'packaged notices need package validation' @('THIRD-PARTY-NOTICES.md') $false $false $true
Assert-Lanes 'ordinary prose and instructions' @('AGENTS.md', 'PLAN.md', 'docs/operations.md', '.github/ISSUE_TEMPLATE/bug.md') $false $false
Assert-Lanes 'fixture provenance prose' @('tests/JetDatabaseWriter.Tests/Databases/Golden/README.md') $false $false
Assert-Lanes 'project prose' @('src/JetDatabaseWriter/README.md', 'tools/JetDatabaseWriter.FormatProbe/README.md', 'benchmarks/README.md') $false $false
Assert-Lanes 'document fixture is executable test input' @('tests/JetDatabaseWriter.Tests/Cfb/Document.docx') $true $false
Assert-Lanes 'non-prose input cannot hide under docs' @('docs/build.props') $true $true
Assert-Lanes 'unknown Markdown location fails closed' @('future/build-input.md') $true $true
Assert-Lanes 'standalone CI helpers use script checks' @('scripts/ci-gate.ps1', 'scripts/submit-candidate.ps1', 'scripts/run-benchmark-comparison.ps1') $false $false
Assert-Lanes 'ordinary script regression uses script checks' @('scripts/tests/ci-gate.tests.ps1') $false $false
Assert-Lanes 'unknown helper fails closed' @('scripts/future-build.ps1') $true $true
Assert-Lanes 'copied fixture helper changes test runner' @('scripts/new-native-calculated-choose-fixtures.ps1') $true $false
Assert-Lanes 'AOT and restore tooling' @('scripts/test-aot.ps1', 'scripts/restore-dependencies.ps1', 'scripts/tests/test-aot.tests.ps1') $true $false
Assert-Lanes 'generated consumer inputs' @('scripts/AotSmoke/Program.cs') $true $false
Assert-Lanes 'proof-only edit' @('verification/Pages/Paging/UndoJournal.dfy', 'verification/config/proof-review.json', 'docs/design/formal-verification.md') $false $true
Assert-Lanes 'proof outside manifest directory cannot hide under docs' @('docs/Unlisted.dfy') $false $true
Assert-Lanes 'hidden proof with alternate extension case' @('docs/.hidden.DFY') $false $true
Assert-Lanes 'proof tooling' @('scripts/verify-proofs.ps1', 'scripts/tests/verify-proofs.tests.ps1') $false $true
Assert-Lanes 'routing checks do not rebuild unchanged test binaries' @('scripts/select-ci-lanes.ps1', 'scripts/tests/select-ci-lanes.tests.ps1') $false $true
Assert-Lanes 'ordinary test edit' @('tests/JetDatabaseWriter.Tests/Writer/WriterTests.cs') $true $false
Assert-Lanes 'handwritten library edit uses unconditional proof review' @('src/JetDatabaseWriter/AccessReader.cs') $true $false
Assert-Lanes 'generated kernel edit' @('src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs') $true $true
Assert-Lanes 'native fixture edit' @('Databases/Golden/Jet4.bin') $true $false
Assert-Lanes 'build configuration affects both' @('Directory.Build.props') $true $true
Assert-Lanes 'SDK pin affects both' @('global.json') $true $true
Assert-Lanes 'workflow routing affects both' @('.github/workflows/ci.yml') $true $true
Assert-Lanes 'shared matrix affects execution and inherited evidence' @('scripts/ci-matrix.ps1') $true $true
Assert-Lanes 'matrix coverage regression changes proof trust' @('scripts/tests/ci-matrix.tests.ps1') $false $true
Assert-Lanes 'workflow coverage regression changes proof trust' @('scripts/tests/ci-workflow.tests.ps1', 'scripts/tests/ci-tools.tests.ps1', 'scripts/tests/test-scripts.tests.ps1') $false $true
Assert-Lanes 'new unclassified file fails closed' @('future/build-input.txt') $true $true
Assert-Lanes 'rename includes removed source path' @('src/JetDatabaseWriter/Old.cs', 'docs/Old.md') $true $false
Assert-Lanes 'quoted unusual path fails closed' @('"docs/line\nname.md"') $true $true
Assert-Lanes 'mixed push unions lanes' @('tests/JetDatabaseWriter.Tests/Example.cs', 'verification/Example.dfy') $true $true
Assert-Lanes 'mixed prose and source retains source coverage' @('README.md', 'tests/JetDatabaseWriter.Tests/Example.cs') $true $false $true

# Keep role exceptions synchronized with actual package and runner inputs.
$libraryProject = [xml][IO.File]::ReadAllText((Join-Path $repoRoot 'src/JetDatabaseWriter/JetDatabaseWriter.csproj'))
foreach ($item in $libraryProject.SelectNodes('//None[@Pack="true"]')) {
    $path = ([string]$item.Include).Replace('\', '/') -replace '^\.\./\.\./', ''
    if ($path -cmatch '\.md$') { Assert-Lanes "packed Markdown $path" @($path) $false $false $true }
}
$testProject = [xml][IO.File]::ReadAllText((Join-Path $repoRoot 'tests/JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj'))
foreach ($item in $testProject.SelectNodes('//None[@CopyToOutputDirectory]')) {
    $path = ([string]$item.Include).Replace('\', '/') -replace '^\.\./\.\./', ''
    if ($path -cmatch '^scripts/.*\.ps1$') { Assert-Lanes "copied runner script $path" @($path) $true $false }
}

$sha = '1' * 40
$base = '2' * 40
$script:runs = @()
$script:ancestor = $true
$script:apiFails = $false
$script:gitFails = $false
$script:jobs = @()
$script:jobCount = $null
$script:gitCalls = [Collections.Generic.List[string]]::new()
$script:changedPaths = @('docs/operations.md')
function Invoke-RestMethod {
    param([string] $Uri, $Headers)
    if ($script:apiFails) { throw 'API unavailable' }
    if ($Uri -match '/actions/runs/\d+/jobs\?filter=latest&per_page=100$') {
        return @{ jobs = $script:jobs; total_count = $(if ($null -eq $script:jobCount) { $script:jobs.Count } else { $script:jobCount }) }
    }
    if ($Uri -notmatch '/actions/workflows/ci.yml/runs\?branch=main&per_page=100$') { throw "Unexpected API request: $Uri" }
    return @{ workflow_runs = $script:runs }
}
function git {
    $script:gitCalls.Add(($args -join ' '))
    $global:LASTEXITCODE = 0
    if ($script:gitFails) { $global:LASTEXITCODE = 128; return }
    if (($args -join ' ') -eq "-c core.quotepath=true diff --no-renames --name-only $base $sha") { return $script:changedPaths }
    if (($args -join ' ') -notmatch "^merge-base --is-ancestor [0-9a-f]{40} $sha`$") { throw "Unexpected Git call: $args" }
    if (-not $script:ancestor) { $global:LASTEXITCODE = 1 }
}
function New-Run([string] $Status = 'completed', [string] $Conclusion = 'success', [string] $Event = 'push', [string] $Branch = 'main', [string] $Path = '.github/workflows/ci.yml') {
    return @{ id = 1; head_sha = $base; head_branch = $Branch; status = $Status; conclusion = $Conclusion; event = $Event; path = $Path; html_url = 'https://github.com/example/repo/actions/runs/1' }
}
function Assert-Baseline([string] $Name, $Expected, [string] $Event = 'push', [string] $Ref = 'refs/heads/main', [bool] $Full = $false, [string] $Lane = 'dotnet') {
    $result = Get-CiBaseline $Event $Ref $sha 'example/repo' 'fixture-token' $Full $Lane
    if ($result.sha -ne $Expected) { throw "$Name returned '$($result.sha)', expected '$Expected'." }
    if ($result -and ($result.runId -lt 1 -or $result.url -ne "https://github.com/example/repo/actions/runs/$($result.runId)" -or $result.evidence -notin 'workflow-success', 'lane-success')) { throw 'Missing exact baseline provenance.' }
    "PASS: $Name"
}
$script:runs = @(New-Run)
Assert-Baseline 'successful main ancestor' $base
Assert-Baseline 'manual dispatch always runs all lanes' $null 'workflow_dispatch'
Assert-Baseline 'release workflow always runs all lanes' $null 'push' 'refs/tags/v4.0.0'
Assert-Baseline 'reusable workflow on main also runs all lanes' $null -Full $true
Assert-Baseline 'PR merge validates both branches since a green ancestor' $base 'pull_request' 'refs/pull/1/merge'
$script:runs = @(New-Run -Status 'in_progress' -Conclusion '')
Assert-Baseline 'pending run cannot authorize skips' $null
$script:runs = @(New-Run -Conclusion 'failure')
Assert-Baseline 'failed run cannot authorize skips' $null
$script:runs = @(New-Run -Event 'pull_request')
Assert-Baseline 'PR result is not a trusted main baseline' $null
$script:runs = @(New-Run -Branch 'feature')
Assert-Baseline 'work branch result cannot authorize skips' $null
$script:runs = @(New-Run -Path '.github/workflows/other.yml')
Assert-Baseline 'different workflow cannot authorize skips' $null
$script:runs = @(New-Run)
$script:ancestor = $false
Assert-Baseline 'force push cannot reuse unrelated success' $null
$script:ancestor = $true
$script:gitFails = $true
Assert-Baseline 'missing history runs all lanes' $null
$script:gitFails = $false
$script:apiFails = $true
Assert-Baseline 'API failure runs all lanes' $null
$script:apiFails = $false
$failed = New-Run -Conclusion 'failure'
$failed.id = 2
$failed.head_sha = '3' * 40
$script:runs = @($failed, (New-Run))
Assert-Baseline 'unvalidated source remains in diff after later documentation pushes' $base
$failed.head_sha = $base
Assert-Baseline 'newer failure supersedes older success for identical SHA' $null
$failed.status = 'in_progress'
$failed.conclusion = ''
Assert-Baseline 'newer pending run supersedes older success for identical SHA' $null

$partial = New-Run -Conclusion 'failure'
$partial.id = 2
$partial.head_sha = '3' * 40
$script:runs = @($partial, (New-Run))
$dotnetNames = @(
    'Analyze (library)'
    'Analyze (tools)'
    'Analyze (tests native)'
    'Analyze (tests standard)'
    foreach ($runtime in 'net10.0', 'net10.0 netstandard', 'net11.0') {
        foreach ($partition in 'ordinary', 'schema-writer', 'jet3', 'jet4', 'ace') { "Test (tests $runtime $partition)" }
    }
    'Trimmed and NativeAOT consumers'
    'Native Access compatibility (net10.0)', 'Native Access compatibility (net10.0-netstandard)', 'Native Access compatibility (net11.0)'
    'Encryption replacement (ubuntu-latest, net10.0)', 'Encryption replacement (ubuntu-latest, net11.0)',
    'Encryption replacement (macos-latest, net10.0)', 'Encryption replacement (macos-latest, net11.0)'
)
if (@(Compare-Object $dotnetNames @(Get-CiLaneJobs 'dotnet')).Count -ne 0) { throw 'The shared analyzer matrix changed the independently reviewed ancestor inventory.' }
$script:jobs = @(@('Select CI lanes', 'Script, security and proof review checks') + $dotnetNames | ForEach-Object { @{ name = $_; status = 'completed'; conclusion = 'success' } })
$script:jobs += @{ name = 'Verify recovery models (Dafny)'; status = 'completed'; conclusion = 'failure' }
$script:jobs += @{ name = 'CI gate'; status = 'completed'; conclusion = 'failure' }
Assert-Baseline 'complete dotnet success survives independent proof failure' $partial.head_sha
$script:jobs += @{ name = 'Package documentation'; status = 'completed'; conclusion = 'skipped' }
Assert-Baseline 'full dotnet pack permits skipped documentation pack' $partial.head_sha
foreach ($runtime in 'net10.0', 'net10.0 netstandard', 'net11.0') {
    foreach ($partition in 'ordinary', 'schema-writer', 'jet3', 'jet4', 'ace') {
        $name = "Test (tests $runtime $partition)"
        $selectedJob = $script:jobs | Where-Object name -CEQ $name
        foreach ($verdict in 'failure', 'skipped', 'cancelled') {
            $selectedJob.conclusion = $verdict
            Assert-Baseline "$name cannot inherit '$verdict'" $base
        }
        $selectedJob.conclusion = 'success'
        $script:jobs = @($script:jobs | Where-Object name -CNE $name)
        Assert-Baseline "$name cannot inherit missing partition" $base
        $script:jobs += $selectedJob
    }
}
foreach ($runtime in 'net10.0', 'net10.0-netstandard', 'net11.0') {
    $name = "Native Access compatibility ($runtime)"
    $selectedJob = $script:jobs | Where-Object name -CEQ $name
    foreach ($verdict in 'failure', 'skipped', 'cancelled') {
        $selectedJob.conclusion = $verdict
        Assert-Baseline "$name cannot inherit '$verdict'" $base
    }
    $selectedJob.conclusion = 'success'
    $script:jobs = @($script:jobs | Where-Object name -CNE $name)
    Assert-Baseline "$name cannot inherit missing native evidence" $base
    $script:jobs += $selectedJob
}
Assert-Baseline 'failed proof still needs an older verified ancestor' $base -Lane 'formal'
($script:jobs | Where-Object name -EQ 'Verify recovery models (Dafny)').status = 'queued'
Assert-Baseline 'rerun racing with metadata cannot certify a pending lane' $base
($script:jobs | Where-Object name -EQ 'Verify recovery models (Dafny)').status = 'completed'
$script:jobs += @{ name = 'CI gate'; status = 'completed'; conclusion = 'failure' }
Assert-Baseline 'duplicate opposite or aggregate job names cannot mask new obligations' $base
$script:jobs = @($script:jobs | Where-Object name -NE 'CI gate')
$script:jobs += @{ name = 'CI gate'; status = 'completed'; conclusion = 'failure' }
($script:jobs | Where-Object name -EQ 'Verify recovery models (Dafny)').conclusion = 'success'
($script:jobs | Where-Object name -EQ 'Test (tests net11.0 ordinary)').conclusion = 'failure'
Assert-Baseline 'proof success survives independent dotnet failure' $partial.head_sha -Lane 'formal'
($script:jobs | Where-Object name -EQ 'Package documentation').conclusion = 'failure'
Assert-Baseline 'proof success survives independent documentation pack failure' $partial.head_sha -Lane 'formal'
($script:jobs | Where-Object name -EQ 'Package documentation').status = 'queued'
Assert-Baseline 'pending documentation job cannot supply completed lane evidence' $base -Lane 'formal'
($script:jobs | Where-Object name -EQ 'Package documentation').status = 'completed'
($script:jobs | Where-Object name -EQ 'Package documentation').conclusion = 'skipped'
Assert-Baseline 'one failed runtime prevents dotnet inheritance' $base
($script:jobs | Where-Object name -EQ 'Test (tests net11.0 ordinary)').conclusion = 'success'
foreach ($verdict in 'failure', 'skipped', 'cancelled', '') {
    ($script:jobs | Where-Object name -EQ 'Analyze (library)').conclusion = $verdict
    Assert-Baseline "dotnet requires every library gate: '$verdict'" $base
}
($script:jobs | Where-Object name -EQ 'Analyze (library)').conclusion = 'success'
($script:jobs | Where-Object name -EQ 'Script, security and proof review checks').conclusion = 'failure'
Assert-Baseline 'failed common checks prevent dotnet inheritance' $base
Assert-Baseline 'failed common checks prevent proof inheritance' $base -Lane 'formal'
($script:jobs | Where-Object name -EQ 'Script, security and proof review checks').conclusion = 'success'
$script:jobs += @{ name = 'New gate'; status = 'completed'; conclusion = 'failure' }
Assert-Baseline 'unclassified failure prevents dotnet inheritance' $base
Assert-Baseline 'unclassified failure prevents proof inheritance' $base -Lane 'formal'
$script:jobs = @($script:jobs | Where-Object name -NE 'New gate')
$script:jobCount = $script:jobs.Count + 1
Assert-Baseline 'incomplete job inventory cannot certify a lane' $base
$script:jobCount = $null
$script:jobs = @($script:jobs | Where-Object name -NE 'Analyze (library)')
Assert-Baseline 'missing analyzer cannot certify dotnet' $base
$script:jobs += @{ name = 'Analyze (library)'; status = 'completed'; conclusion = 'success' }
$script:jobs += @{ name = 'Analyze (library)'; status = 'completed'; conclusion = 'success' }
Assert-Baseline 'duplicate analyzer cannot certify dotnet' $base

# Reject a missing or extra lane result even when a workflow would otherwise report success.
Assert-CiResults @{ dotnet = $false; formal = $true; package = $false } @{ changes = 'success'; checks = 'success'; formal = 'success'; analyze = 'skipped'; test = 'skipped'; aot = 'skipped'; 'native-access' = 'skipped'; 'encryption-replacement' = 'skipped'; package = 'skipped' }
Assert-CiResults @{ dotnet = $false; formal = $false; package = $true } @{ changes = 'success'; checks = 'success'; formal = 'skipped'; analyze = 'skipped'; test = 'skipped'; aot = 'skipped'; 'native-access' = 'skipped'; 'encryption-replacement' = 'skipped'; package = 'success' }
foreach ($dotnet in $false, $true) {
    foreach ($formal in $false, $true) {
        foreach ($package in $false, $true) {
            if ($dotnet -and $package) { continue }
            $results = @{ changes = 'success'; checks = 'success'; formal = $(if ($formal) { 'success' } else { 'skipped' }); package = $(if ($package) { 'success' } else { 'skipped' }) }
            foreach ($job in 'analyze', 'test', 'aot', 'native-access', 'encryption-replacement') { $results[$job] = if ($dotnet) { 'success' } else { 'skipped' } }
            Assert-CiResults @{ dotnet = $dotnet; formal = $formal; package = $package } $results
        }
    }
}
foreach ($bad in 'failure', 'cancelled', 'skipped', '') {
    $caught = $false
    try { Assert-CiResults @{ dotnet = $true; formal = $true; package = $false } @{ changes = 'success'; checks = 'success'; formal = 'success'; analyze = $bad; test = 'success'; aot = 'success'; 'native-access' = 'success'; 'encryption-replacement' = 'success'; package = 'skipped' } } catch { $caught = $true }
    if (-not $caught) { throw "Aggregate gate accepted required analyzer verdict '$bad'." }
}
'PASS: aggregate gate requires all selected lanes'
foreach ($bad in 'failure', 'cancelled', 'skipped', '') {
    $caught = $false
    try { Assert-CiResults @{ dotnet = $true; formal = $false; package = $false } @{ changes = 'success'; checks = 'success'; formal = 'skipped'; analyze = 'success'; test = $bad; aot = 'success'; 'native-access' = 'success'; 'encryption-replacement' = 'success'; package = 'skipped' } } catch { $caught = $true }
    if (-not $caught) { throw "Aggregate gate accepted required test execution verdict '$bad'." }
}
'PASS: aggregate gate requires execution of the analyzed test assets'
foreach ($bad in 'failure', 'cancelled', 'skipped', '') {
    $caught = $false
    try { Assert-CiResults @{ dotnet = $true; formal = $false; package = $false } @{ changes = 'success'; checks = 'success'; formal = 'skipped'; analyze = 'success'; test = 'success'; aot = 'success'; 'native-access' = $bad; 'encryption-replacement' = 'success'; package = 'skipped' } } catch { $caught = $true }
    if (-not $caught) { throw "Aggregate gate accepted required native compatibility verdict '$bad'." }
}
'PASS: aggregate gate requires native compatibility'
foreach ($bad in 'failure', 'cancelled', 'skipped', '') {
    $caught = $false
    try { Assert-CiResults @{ dotnet = $false; formal = $false; package = $true } @{ changes = 'success'; checks = 'success'; formal = 'skipped'; analyze = 'skipped'; test = 'skipped'; aot = 'skipped'; 'native-access' = 'skipped'; 'encryption-replacement' = 'skipped'; package = $bad } } catch { $caught = $true }
    if (-not $caught) { throw "Aggregate gate accepted required documentation package verdict '$bad'." }
}
'PASS: aggregate gate requires documentation packaging when selected'

# Exercise real output serialization and receipt generation without GitHub or Git.
$temporary = Join-Path ([IO.Path]::GetTempPath()) ('jdw-ci-matrix-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($temporary)
$variables = @('GITHUB_EVENT_NAME', 'GITHUB_REF', 'GITHUB_SHA', 'GITHUB_REPOSITORY', 'GITHUB_TOKEN', 'JDW_CI_FORCE_FULL', 'GITHUB_OUTPUT', 'GITHUB_STEP_SUMMARY', 'RUNNER_TEMP', 'JDW_CI_NEEDS')
$saved = @{}
foreach ($name in $variables) { $saved[$name] = [Environment]::GetEnvironmentVariable($name) }
try {
    $env:GITHUB_REF = 'refs/heads/main'
    $env:GITHUB_SHA = $sha
    $env:GITHUB_REPOSITORY = 'example/repo'
    $env:GITHUB_TOKEN = 'fixture-token'
    $env:GITHUB_OUTPUT = Join-Path $temporary 'outputs.txt'
    $env:GITHUB_STEP_SUMMARY = Join-Path $temporary 'summary.md'
    $env:RUNNER_TEMP = $temporary
    $script:runs = @(New-Run)
    foreach ($event in 'workflow_dispatch', 'workflow_call', 'push', 'pull_request') {
        $env:GITHUB_EVENT_NAME = $event
        $env:JDW_CI_FORCE_FULL = ($event -eq 'workflow_call').ToString().ToLowerInvariant()
        [IO.File]::WriteAllText($env:GITHUB_OUTPUT, '')
        . $HelperPath | Out-Null
        $outputs = @{}
        foreach ($line in [IO.File]::ReadAllLines($env:GITHUB_OUTPUT)) {
            $field = $line.Split('=', 2)
            $outputs[$field[0]] = $field[1]
        }
        $expanded = $outputs.matrix | ConvertFrom-Json -AsHashtable
        if (@($expanded.include).Count -ne 4 -or @($expanded.include | Where-Object runner -EQ $true).Count -ne 2) { throw 'Serialized matrix lost the analyzed assets.' }
        $testMatrix = $outputs['test-matrix'] | ConvertFrom-Json -AsHashtable
        if (@($testMatrix.include).Count -ne 15 -or @($testMatrix.include | Where-Object asset -CEQ 'standard').Count -ne 5) { throw 'Serialized execution matrix lost runtime partitions or asset bindings.' }
        $receipt = [IO.File]::ReadAllText((Join-Path $temporary 'ci-selection.json')) | ConvertFrom-Json -AsHashtable
        $selected = $event -notin 'push', 'pull_request'
        if ($outputs.dotnet -ne $selected.ToString().ToLowerInvariant() -or $outputs.formal -ne $outputs.dotnet -or $outputs.package -ne 'false' -or $receipt.revision -ne $sha -or $receipt.dotnet -ne $selected -or $receipt.formal -ne $selected -or $receipt.package) { throw "Incorrect selection output or receipt for ${event}: $($receipt | ConvertTo-Json -Depth 4 -Compress)." }
        if (-not $selected -and ($receipt.baselines.dotnet.sha -ne $base -or $receipt.baselines.formal.sha -ne $base)) { throw 'Skipped lanes lost validated ancestor evidence.' }
        "PASS: $event emits the complete matrix and exact selection receipt"
    }
    $env:GITHUB_EVENT_NAME = 'push'
    $env:JDW_CI_FORCE_FULL = 'false'
    foreach ($case in @(
        @{ paths = @('README.md'); dotnet = $false; formal = $false; package = $true },
        @{ paths = @('README.md', 'src/JetDatabaseWriter/AccessReader.cs'); dotnet = $true; formal = $false; package = $false },
        @{ paths = @('README.md', 'verification/Pages/Paging/UndoJournal.dfy'); dotnet = $false; formal = $true; package = $true },
        @{ paths = @('tests/JetDatabaseWriter.Tests/Databases/Golden/README.md'); dotnet = $false; formal = $false; package = $false },
        @{ paths = @('scripts/ci-gate.ps1'); dotnet = $false; formal = $false; package = $false },
        @{ paths = @('docs/build.props'); dotnet = $true; formal = $true; package = $false }
    )) {
        $script:changedPaths = $case.paths
        . $HelperPath | Out-Null
        $receipt = [IO.File]::ReadAllText((Join-Path $temporary 'ci-selection.json')) | ConvertFrom-Json -AsHashtable
        foreach ($lane in 'dotnet', 'formal', 'package') {
            if ($receipt[$lane] -ne $case[$lane]) { throw "Incorrect $lane selection for $($case.paths -join ', ')." }
        }
        if (($receipt.paths.dotnet -join ',') -ne ($case.paths -join ',') -or $receipt.baselines.dotnet.sha -ne $base) { throw 'Content selection lost complete changed paths or baseline provenance.' }
        "PASS: content selection receipt for $($case.paths -join ', ')"
    }
    $env:JDW_CI_NEEDS = @{
        changes = @{ result = 'success'; outputs = @{ dotnet = 'false'; formal = 'true'; package = 'false' } }
        checks = @{ result = 'success' }
        formal = @{ result = 'success' }
        analyze = @{ result = 'skipped' }
        test = @{ result = 'skipped' }
        aot = @{ result = 'skipped' }
        'native-access' = @{ result = 'skipped' }
        'encryption-replacement' = @{ result = 'skipped' }
        package = @{ result = 'skipped' }
    } | ConvertTo-Json -Depth 4
    . $HelperPath -CheckResults | Out-Null
    'PASS: aggregate entry point accepts only the selected lanes'
    foreach ($case in @(
        @{ dotnet = 'false'; formal = 'true' },
        @{ dotnet = 'true'; formal = 'true'; package = 'true' }
    )) {
        $needs = $env:JDW_CI_NEEDS | ConvertFrom-Json -AsHashtable
        $needs.changes.outputs = $case
        $env:JDW_CI_NEEDS = $needs | ConvertTo-Json -Depth 4
        $caught = $false
        try { . $HelperPath -CheckResults | Out-Null } catch { $caught = $true }
        if (-not $caught) { throw 'Aggregate entry point accepted missing or duplicate package coverage.' }
    }
    'PASS: aggregate entry point rejects missing package selection and duplicate packaging'

    $failing = Join-Path $temporary 'failing'
    [void][IO.Directory]::CreateDirectory($failing)
    Copy-Item -LiteralPath $HelperPath -Destination (Join-Path $failing 'select-ci-lanes.ps1')
    [IO.File]::WriteAllText((Join-Path $failing 'ci-matrix.ps1'), "function Get-CiAnalyzerMatrix { throw 'matrix generation failed' }")
    $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
    $caught = $null
    try { . (Join-Path $failing 'select-ci-lanes.ps1') | Out-Null } catch { $caught = $_.Exception.Message }
    if ($caught -ne 'matrix generation failed') { throw 'A generator failure must fail selection rather than authorize an empty analyzer matrix.' }
    'PASS: matrix generation failure fails selection'
}
finally {
    foreach ($name in $variables) { [Environment]::SetEnvironmentVariable($name, $saved[$name]) }
    [IO.Directory]::Delete($temporary, $true)
}
