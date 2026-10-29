# Path selection must retain coverage across failed, pending and multi-commit pushes.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile((Join-Path $repoRoot 'scripts/select-ci-lanes.ps1'), [ref]$null, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors.Message -join '; ') }
foreach ($definition in $ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
    . ([scriptblock]::Create($definition.Extent.Text))
}

function Assert-Lanes([string] $Name, [string[]] $Paths, [bool] $Dotnet, [bool] $Formal) {
    $actual = Get-CiLanes $Paths
    if ($actual.dotnet -ne $Dotnet -or $actual.formal -ne $Formal) { throw "$Name selected incorrect lanes: $($actual | ConvertTo-Json -Compress)" }
    "PASS: $Name"
}
Assert-Lanes 'unchanged validated tree' @() $false $false
Assert-Lanes 'readme only' @('README.md') $false $false
Assert-Lanes 'proof-only edit' @('verification/UndoJournal.dfy', 'verification/proof-review.json', 'docs/design/formal-verification.md') $false $true
Assert-Lanes 'proof outside manifest directory cannot hide under docs' @('docs/Unlisted.dfy') $false $true
Assert-Lanes 'hidden proof with alternate extension case' @('docs/.hidden.DFY') $false $true
Assert-Lanes 'proof tooling' @('scripts/verify-proofs.ps1', 'scripts/tests/verify-proofs.tests.ps1') $false $true
Assert-Lanes 'ordinary test edit' @('tests/JetDatabaseWriter.Tests/Writer/WriterTests.cs') $true $false
Assert-Lanes 'handwritten library edit uses unconditional proof review' @('src/JetDatabaseWriter/AccessReader.cs') $true $false
Assert-Lanes 'generated kernel edit' @('src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs') $true $true
Assert-Lanes 'native fixture edit' @('Databases/Golden/Jet4.bin') $true $false
Assert-Lanes 'build configuration affects both' @('Directory.Build.props') $true $true
Assert-Lanes 'SDK pin affects both' @('global.json') $true $true
Assert-Lanes 'workflow routing affects both' @('.github/workflows/ci.yml') $true $true
Assert-Lanes 'new unclassified file fails closed' @('future/build-input.txt') $true $true
Assert-Lanes 'rename includes removed source path' @('src/JetDatabaseWriter/Old.cs', 'docs/Old.md') $true $false
Assert-Lanes 'quoted unusual path fails closed' @('"docs/line\nname.md"') $true $true
Assert-Lanes 'mixed push unions lanes' @('tests/JetDatabaseWriter.Tests/Example.cs', 'verification/Example.dfy') $true $true

$sha = '1' * 40
$base = '2' * 40
$script:runs = @()
$script:ancestor = $true
$script:apiFails = $false
$script:gitFails = $false
$script:gitCalls = [Collections.Generic.List[string]]::new()
function Invoke-RestMethod {
    param([string] $Uri, $Headers)
    if ($script:apiFails) { throw 'API unavailable' }
    if ($Uri -notmatch '/actions/workflows/ci.yml/runs\?branch=main&per_page=100$') { throw "Unexpected API request: $Uri" }
    return @{ workflow_runs = $script:runs }
}
function git {
    $script:gitCalls.Add(($args -join ' '))
    $global:LASTEXITCODE = 0
    if ($script:gitFails) { $global:LASTEXITCODE = 128; return }
    if (($args -join ' ') -ne "merge-base --is-ancestor $base $sha") { throw "Unexpected Git call: $args" }
    if (-not $script:ancestor) { $global:LASTEXITCODE = 1 }
}
function New-Run([string] $Status = 'completed', [string] $Conclusion = 'success', [string] $Event = 'push', [string] $Branch = 'main', [string] $Path = '.github/workflows/ci.yml') {
    return @{ id = 1; head_sha = $base; head_branch = $Branch; status = $Status; conclusion = $Conclusion; event = $Event; path = $Path; html_url = 'https://github.com/example/repo/actions/runs/1' }
}
function Assert-Baseline([string] $Name, $Expected, [string] $Event = 'push', [string] $Ref = 'refs/heads/main', [bool] $Full = $false) {
    $result = Get-CiBaseline $Event $Ref $sha 'example/repo' 'fixture-token' $Full
    if ($result -ne $Expected) { throw "$Name returned '$result', expected '$Expected'." }
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

# Reject a missing or extra lane result even when a workflow would otherwise report success.
Assert-CiResults @{ dotnet = $false; formal = $true } @{ changes = 'success'; checks = 'success'; formal = 'success'; analyze = 'skipped'; aot = 'skipped'; 'encryption-replacement' = 'skipped' }
foreach ($bad in 'failure', 'cancelled', 'skipped', '') {
    $caught = $false
    try { Assert-CiResults @{ dotnet = $true; formal = $true } @{ changes = 'success'; checks = 'success'; formal = 'success'; analyze = $bad; aot = 'success'; 'encryption-replacement' = 'success' } } catch { $caught = $true }
    if (-not $caught) { throw "Aggregate gate accepted required analyzer verdict '$bad'." }
}
'PASS: aggregate gate requires all selected lanes'
