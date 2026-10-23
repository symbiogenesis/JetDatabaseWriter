# Isolated proof-gate regressions use synthetic sources and mocked verifier results.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-tests-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($fixture)
function Import-ScriptFunctions([string] $Path) {
    $errors = $null
    $ast = [Management.Automation.Language.Parser]::ParseFile($Path, [ref]$null, [ref]$errors)
    if ($errors.Count -gt 0) { throw ($errors.Message -join '; ') }
    foreach ($definition in $ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
        . ([scriptblock]::Create($definition.Extent.Text))
        Set-Item -Path ('function:script:' + $definition.Name) -Value (Get-Item ('function:' + $definition.Name)).ScriptBlock
    }
}
Import-ScriptFunctions (Join-Path $repoRoot 'scripts/verify-proofs.ps1')
Import-ScriptFunctions (Join-Path $repoRoot 'scripts/install-dafny.ps1')
function Assert-Scenario([string] $Name, [scriptblock] $Action, [string] $ExpectedError) {
    $caught = $null
    try { & $Action | Out-Null } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or -not $caught.Contains($ExpectedError)) { throw "$Name expected '$ExpectedError', got '$caught'." }
    } elseif ($caught) { throw "$Name failed: $caught" }
    "PASS: $Name"
}
function Write-Fixture([string] $Relative, [string] $Content) {
    $path = Join-Path $fixture $Relative
    [void][IO.Directory]::CreateDirectory((Split-Path $path -Parent))
    [IO.File]::WriteAllText($path, $Content)
    return $path
}
$source = Write-Fixture 'verification/Good.dfy' 'lemma Good() ensures 1 == 1 {}'
$manifest = Write-Fixture 'verification/proofs.json' '{"schemaVersion":1,"files":["verification/Good.dfy"]}'
Assert-Scenario 'complete exact inventory' { Get-ProofFiles $fixture $manifest }
Assert-Scenario 'omitted proof' {
    $extra = Write-Fixture 'Hidden.dfy' 'lemma Hidden() {}'
    try { Get-ProofFiles $fixture $manifest } finally { [IO.File]::Delete($extra) }
} 'Proof inventory mismatch'
Assert-Scenario 'missing proof' { Get-ProofFiles $fixture (Write-Fixture 'missing.json' '{"schemaVersion":1,"files":["verification/Missing.dfy"]}') } 'Proof inventory mismatch'
Assert-Scenario 'duplicate proof' { Get-ProofFiles $fixture (Write-Fixture 'duplicate.json' '{"schemaVersion":1,"files":["verification/Good.dfy","verification/Good.dfy"]}') } 'Duplicate proof'
Assert-Scenario 'relative traversal' { Get-ProofFiles $fixture (Write-Fixture 'escape.json' '{"schemaVersion":1,"files":["verification/../Good.dfy"]}') } 'Invalid proof path'
Assert-Scenario 'empty proof inventory' { Get-ProofFiles $fixture (Write-Fixture 'empty.json' '{"schemaVersion":1,"files":[]}') } 'nonempty'
Assert-Scenario 'comments and strings do not become policy tokens' { Assert-ProofSource '// assume include {:axiom}' "lemma Good() { var x := `"assume`"; /* nested /* assume */ include */ }" }
foreach ($sourceText in 'lemma X() { assume false; }', 'include "other.dfy"', 'lemma {:verify false} X() {}', 'lemma {:axiom} X() ensures false', 'lemma {:extern} X() ensures false', 'lemma {:options "--no-verify"} X() {}', 'method X() decreases * {}', '@Options("--no-verify") lemma X() {}') {
    Assert-Scenario "reject source bypass $sourceText" { Assert-ProofSource 'control.dfy' $sourceText } 'Disallowed proof construct'
}
Assert-Scenario 'trigger and induction proof hints allowed' { Assert-ProofSource 'hints.dfy' 'lemma {:induction false} X() { assert forall i: int {:trigger F(i)} :: F(i) == F(i); }' }
$report = Write-Fixture 'audit.txt' ''
$success = [pscustomobject]@{ ExitCode = 0; Stdout = 'Dafny program verifier finished with 2 verified, 0 errors'; Stderr = '' }
Assert-Scenario 'successful verification summary' { Assert-ProofResult $success 'control' }
Assert-Scenario 'empty verification rejected' { Assert-ProofResult ([pscustomobject]@{ ExitCode=0; Stdout='Dafny program verifier finished with 0 verified, 0 errors'; Stderr='' }) 'control' } 'positive verification count'
Assert-Scenario 'exit code failure rejected' { Assert-ProofResult ([pscustomobject]@{ ExitCode=4; Stdout='2 verified, 0 errors'; Stderr='' }) 'control' } 'failed'
Assert-Scenario 'missing verification summary rejected' { Assert-ProofResult ([pscustomobject]@{ ExitCode=0; Stdout=''; Stderr='' }) 'control' } 'positive verification count'
Assert-Scenario 'clean audit' { Assert-ProofAudit ([pscustomobject]@{ ExitCode=0; Stdout='Dafny auditor completed with 0 findings'; Stderr='' }) $report 'control' }
Assert-Scenario 'zero-exit audit finding rejected' {
    [IO.File]::WriteAllText($report, 'Assumed postcondition')
    try { Assert-ProofAudit ([pscustomobject]@{ ExitCode=0; Stdout='Dafny auditor completed with 1 findings'; Stderr='' }) $report 'control' } finally { [IO.File]::WriteAllText($report, '') }
} 'audit findings'
Assert-Scenario 'missing audit report rejected' { Assert-ProofAudit ([pscustomobject]@{ ExitCode=0; Stdout='Dafny auditor completed with 0 findings'; Stderr='' }) (Join-Path $fixture 'absent.txt') 'control' } 'audit report'
Assert-Scenario 'missing audit summary rejected' { Assert-ProofAudit ([pscustomobject]@{ ExitCode=0; Stdout=''; Stderr='' }) $report 'control' } 'audit findings'
Assert-Scenario 'relative output rejected' { Assert-ProofOutputDirectory 'relative/path' $fixture } 'absolute'
Assert-Scenario 'unresolved environment token rejected' { Assert-ProofOutputDirectory '%TEMP%/proofs' $fixture } 'environment'
Assert-Scenario 'in-repository output rejected' { Assert-ProofOutputDirectory (Join-Path $fixture 'logs') $fixture } 'outside'
Assert-Scenario 'archive hash mismatch rejected before extraction' { Assert-DafnyArchive $source ('0' * 64) } 'SHA256 mismatch'
# Exercise real process draining and timeout with PowerShell, without invoking Dafny or a network.
$pwsh = Join-Path $PSHOME $(if ($IsWindows) { 'pwsh.exe' } else { 'pwsh' })
Assert-Scenario 'concurrent stdout and stderr draining' {
    $result = Invoke-ProofProcess $pwsh @('-NoProfile', '-Command', '1..5000 | ForEach-Object { [Console]::Out.WriteLine("out"); [Console]::Error.WriteLine("err") }') $fixture 'streams' 20
    if ($result.ExitCode -ne 0 -or $result.Stdout.Length -lt 20000 -or $result.Stderr.Length -lt 20000) { throw 'Output was not fully captured.' }
}
Assert-Scenario 'bounded process timeout' { Invoke-ProofProcess $pwsh @('-NoProfile', '-Command', '[Console]::Out.WriteLine("before timeout"); while ($true) {}') $fixture 'timeout' 1 } 'timed out'
Assert-Scenario 'timeout retains diagnostics' {
    if (-not [IO.File]::ReadAllText((Join-Path $fixture 'timeout.stdout.log')).Contains('before timeout') -or
        -not [IO.File]::ReadAllText((Join-Path $fixture 'timeout.failure.txt')).Contains('timed out')) { throw 'Timeout evidence was not retained.' }
}
Assert-Scenario 'launch failure is bounded' { Invoke-ProofProcess (Join-Path $fixture 'missing-executable') @() $fixture 'launch' 1 } 'Exception calling'
Assert-Scenario 'launch failure retains diagnostics' {
    if (-not [IO.File]::Exists((Join-Path $fixture 'launch.failure.txt'))) { throw 'Launch evidence was not retained.' }
}
'Proof gate regression checks passed.'