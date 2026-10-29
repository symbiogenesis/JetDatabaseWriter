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
$dependent = Write-Fixture 'verification/RecoveryKernel.dfy' 'module RecoveryKernel {}'
$dependencyManifest = Write-Fixture 'dependencies.json' '{"schemaVersion":1,"files":["verification/Good.dfy","verification/RecoveryKernel.dfy"],"dependencies":{"verification/RecoveryKernel.dfy":["verification/Good.dfy"]}}'
Assert-Scenario 'independently verified earlier library' {
    $dependenciesByFile = Get-ProofDependencies $fixture $dependencyManifest @($source, $dependent)
    if ($dependenciesByFile[$dependent].Count -ne 1 -or $dependenciesByFile[$dependent][0] -ne $source) { throw 'Library mapping changed.' }
}
foreach ($libraryCase in @(
    '{"dependencies":{"verification/Absent.dfy":["verification/Good.dfy"]}}',
    '{"dependencies":{"verification/RecoveryKernel.dfy":["verification/Absent.dfy"]}}',
    '{"dependencies":{"verification/Good.dfy":["verification/RecoveryKernel.dfy"]}}',
    '{"dependencies":{"verification/RecoveryKernel.dfy":["verification/RecoveryKernel.dfy"]}}',
    '{"dependencies":{"verification/RecoveryKernel.dfy":["verification/Good.dfy","verification/Good.dfy"]}}',
    '{"dependencies":{"verification/RecoveryKernel.dfy":"verification/Good.dfy"}}'
)) {
    Assert-Scenario "reject unverified or circular library $libraryCase" { Get-ProofDependencies $fixture (Write-Fixture 'bad-library.json' $libraryCase) @($source, $dependent) } 'Invalid proof dependency'
}
$translation = @'
// Unused compiler scaffolding precedes the executable module.
namespace JetDatabaseWriter.Verification.RecoveryKernel {

  public partial class __default {
    public static bool Accept(byte value)
    {
      return (value) == ((byte)(0));
    }
  }
} // end of namespace JetDatabaseWriter.Verification.RecoveryKernel
namespace JetDatabaseWriter.Verification {

} // end of namespace JetDatabaseWriter.Verification

'@
Assert-Scenario 'native projection preserves generated method body' {
    $projected = ConvertFrom-NativeKernelTranslation 'RecoveryKernel' $translation
    if (-not $projected.Contains('return (value) == ((byte)(0));') -or
        -not $projected.Contains('internal static class RecoveryKernel') -or
        $projected.Contains('Unused compiler scaffolding') -or $projected -match '(?<!\r)\n') { throw 'Unexpected projection.' }
}
Assert-Scenario 'changed compiler structure rejected' { ConvertFrom-NativeKernelTranslation 'RecoveryKernel' ($translation.Replace('public partial class __default', 'public class Other')) } 'Unsupported native kernel translation'
Assert-Scenario 'generated qualified self reference follows type packaging' {
    $projected = ConvertFrom-NativeKernelTranslation 'RecoveryKernel' ($translation.Replace('return (value) == ((byte)(0));', 'return JetDatabaseWriter.Verification.RecoveryKernel.__default.Accept(value);'))
    if (-not $projected.Contains('return JetDatabaseWriter.Verification.RecoveryKernel.Accept(value);')) { throw 'Qualified self reference was changed incorrectly.' }
}
Assert-Scenario 'runtime-dependent kernel rejected' { ConvertFrom-NativeKernelTranslation 'RecoveryKernel' ($translation.Replace('return (value) == ((byte)(0));', 'return Dafny.Helpers.Value(value);')) } 'runtime support'
Assert-Scenario 'unexpected compiled module rejected' { ConvertFrom-NativeKernelTranslation 'RecoveryKernel' ($translation + 'namespace Other {}') } 'Unsupported native kernel translation'
$longValueTranslation = $translation.Replace('RecoveryKernel', 'LongValueKernel')
Assert-Scenario 'second native kernel preserves method bodies and self references' {
    $projected = ConvertFrom-NativeKernelTranslation 'LongValueKernel' ($longValueTranslation.Replace('return (value) == ((byte)(0));', 'return JetDatabaseWriter.Verification.LongValueKernel.__default.Accept(value);'))
    if (-not $projected.Contains('internal static class LongValueKernel') -or
        -not $projected.Contains('verification/LongValueKernel.dfy') -or
        -not $projected.Contains('return JetDatabaseWriter.Verification.LongValueKernel.Accept(value);')) { throw 'Second kernel packaging changed incorrectly.' }
}
Assert-Scenario 'only reviewed native kernel names admitted' { ConvertFrom-NativeKernelTranslation 'Other' $translation } 'Unsupported native kernel'
Assert-Scenario 'wrong kernel namespace rejected' { ConvertFrom-NativeKernelTranslation 'LongValueKernel' $translation } 'Unsupported native kernel translation'
Assert-Scenario 'second kernel refuses runtime-dependent translation' { ConvertFrom-NativeKernelTranslation 'LongValueKernel' ($longValueTranslation.Replace('return (value) == ((byte)(0));', 'return LongValueNativeTypes.Value(value);')) } 'runtime support'
$originalProofProcess = ${function:Invoke-ProofProcess}
function Invoke-ProofProcess([string] $Executable, [string[]] $Arguments, [string] $WorkingDirectory, [string] $Name, [int] $TimeoutSeconds) {
    if ($Arguments -contains '--no-verify' -or $Arguments -contains '--include-runtime' -or
        $Arguments -contains '--library' -or $Arguments -notcontains $source) { throw 'Generation must verify with its declared source dependencies and omit the runtime.' }
    $outputIndex = [Array]::IndexOf($Arguments, '--output') + 1
    [IO.File]::WriteAllText($Arguments[$outputIndex], $(if ($Name -eq 'LongValueKernel.translate') { $longValueTranslation } else { $translation }))
    return [pscustomobject]@{ ExitCode = 0; Stdout = 'Dafny program verifier finished with 2 verified, 0 errors'; Stderr = '' }
}
try {
    $generatedPath = Join-Path $fixture 'src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs'
    Assert-Scenario 'missing generated source rejected' { Sync-NativeKernel 'RecoveryKernel' $fixture 'mock' $fixture @($source) @() 10 $false } 'stale'
    Assert-Scenario 'explicit generation then exact freshness check' {
        Sync-NativeKernel 'RecoveryKernel' $fixture 'mock' $fixture @($source) @() 10 $true
        Sync-NativeKernel 'RecoveryKernel' $fixture 'mock' $fixture @($source) @() 10 $false
    }
    Assert-Scenario 'edited generated method rejected' {
        [IO.File]::WriteAllText($generatedPath, ([IO.File]::ReadAllText($generatedPath).Replace('return (value) == ((byte)(0));', 'return true;')))
        Sync-NativeKernel 'RecoveryKernel' $fixture 'mock' $fixture @($source) @() 10 $false
    } 'stale'
    Assert-Scenario 'second native kernel has independent generation and freshness' {
        Sync-NativeKernel 'LongValueKernel' $fixture 'mock' $fixture @($source) @() 10 $true
        Sync-NativeKernel 'LongValueKernel' $fixture 'mock' $fixture @($source) @() 10 $false
        $longValueGeneratedPath = Join-Path $fixture 'src/JetDatabaseWriter/Verification/LongValueKernel.g.cs'
        if (-not [IO.File]::ReadAllText($longValueGeneratedPath).Contains('internal static class LongValueKernel') -or
            -not [IO.File]::ReadAllText($generatedPath).Contains('return true;')) { throw 'Native kernel outputs were not independent.' }
    }
    Assert-Scenario 'edited second kernel rejected independently' {
        $longValueGeneratedPath = Join-Path $fixture 'src/JetDatabaseWriter/Verification/LongValueKernel.g.cs'
        [IO.File]::WriteAllText($longValueGeneratedPath, ([IO.File]::ReadAllText($longValueGeneratedPath).Replace('return (value) == ((byte)(0));', 'return true;')))
        Sync-NativeKernel 'LongValueKernel' $fixture 'mock' $fixture @($source) @() 10 $false
    } 'stale'
} finally { ${function:Invoke-ProofProcess} = $originalProofProcess }
Assert-Scenario 'comments and strings do not become policy tokens' { Assert-ProofSource '// assume include {:axiom}' "lemma Good() { var x := `"assume`"; /* nested /* assume */ include */ }" }
foreach ($sourceText in 'lemma X() { assume false; }', 'include "other.dfy"', 'lemma {:verify false} X() {}', 'lemma {:axiom} X() ensures false', 'lemma {:extern} X() ensures false', 'lemma {:options "--no-verify"} X() {}', 'method X() decreases * {}', '@Options("--no-verify") lemma X() {}') {
    Assert-Scenario "reject source bypass $sourceText" { Assert-ProofSource 'control.dfy' $sourceText } 'Disallowed proof construct'
}
Assert-Scenario 'trigger and induction proof hints allowed' { Assert-ProofSource 'hints.dfy' 'lemma {:induction false} X() { assert forall i: int {:trigger F(i)} :: F(i) == F(i); }' }
Assert-Scenario 'reviewed native integer representations allowed' { Assert-ProofSource 'native.dfy' 'newtype {:nativeType "ulong"} U64 = x: int | 0 <= x < 18446744073709551616 newtype {:nativeType "uint"} U32 = x: int | 0 <= x < 4294967296 newtype {:nativeType "byte"} U8 = x: int | 0 <= x < 256' }
foreach ($nativeType in '"int"', '"number"', 'false', '"ulong" false') {
    Assert-Scenario "unreviewed native representation rejected $nativeType" { Assert-ProofSource 'native.dfy' "newtype {:nativeType $nativeType} Native = x: int | 0 <= x < 256" } 'Disallowed proof construct'
}
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
Assert-Scenario 'process result receipt retains exit and arguments' {
    $receipt = [IO.File]::ReadAllText((Join-Path $fixture 'streams.process.json')) | ConvertFrom-Json
    if ($receipt.exitCode -ne 0 -or $receipt.name -ne 'streams' -or $receipt.arguments -notcontains '-NoProfile') { throw 'Process outcome was not retained.' }
}
$originalVerificationCore = ${function:Invoke-ProofVerificationCore}
function Invoke-ProofVerificationCore([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [int] $TimeoutSeconds, [bool] $Update) {
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'FalseTheorem.dfy'), 'lemma FalseTheorem() ensures false {}')
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'negative-false.stdout.log'), 'postcondition might not hold')
    throw 'synthetic failed proof'
}
try {
    $failureOutput = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-failure-' + [guid]::NewGuid().ToString('N'))
    Assert-Scenario 'failed proof is still rejected' { Invoke-ProofVerification $fixture 'mock' $failureOutput 10 $false } 'synthetic failed proof'
    Assert-Scenario 'failure receipt binds source and retains counterexample diagnostics' {
        $failure = [IO.File]::ReadAllText((Join-Path $failureOutput 'failure.json')) | ConvertFrom-Json
        $saved = Join-Path $failureOutput 'inputs/verification/Good.dfy'
        if ($failure.status -ne 'failed' -or $failure.failure -ne 'synthetic failed proof' -or
            $failure.artifacts -notcontains 'negative-false.stdout.log' -or
            -not [IO.File]::Exists((Join-Path $failureOutput 'FalseTheorem.dfy')) -or
            -not [IO.File]::Exists((Join-Path $failureOutput 'inputs/verification/RecoveryKernel.dfy')) -or
            $failure.negativeControls -notmatch 'genuine gate failure' -or
            [IO.File]::ReadAllText($saved) -ne [IO.File]::ReadAllText($source) -or
            -not (@($failure.inputs | Where-Object path -CEQ 'verification/Good.dfy').sha256 -ceq (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant()) -or
            [IO.File]::Exists((Join-Path $failureOutput 'summary.json'))) { throw 'Failed proof evidence is incomplete or claims success.' }
    }
    Assert-Scenario 'existing result directories remain untouched' { Invoke-ProofVerification $fixture 'mock' $failureOutput 10 $false } 'must not already exist'
} finally { ${function:Invoke-ProofVerificationCore} = $originalVerificationCore }
$originalFailureReceipt = ${function:Write-ProofFailureReceipt}
function Invoke-ProofVerificationCore([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [int] $TimeoutSeconds, [bool] $Update) { throw 'original verification failure' }
function Write-ProofFailureReceipt([string] $RepositoryRoot, [string] $OutputDirectory, [string] $Failure) { throw 'snapshot retention failure' }
try {
    $snapshotOutput = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-snapshot-failure-' + [guid]::NewGuid().ToString('N'))
    Assert-Scenario 'snapshot failure preserves original verification error' { Invoke-ProofVerification $fixture 'mock' $snapshotOutput 10 $false } 'original verification failure'
    Assert-Scenario 'snapshot failure retains a supplemental error' {
        if (-not [IO.File]::ReadAllText((Join-Path $snapshotOutput 'failure-receipt.error.txt')).Contains('snapshot retention failure')) { throw 'Snapshot failure was not retained separately.' }
    }
} finally {
    ${function:Invoke-ProofVerificationCore} = $originalVerificationCore
    ${function:Write-ProofFailureReceipt} = $originalFailureReceipt
}
$linkTarget = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-link-target-' + [guid]::NewGuid().ToString('N') + '.txt')
[IO.File]::WriteAllText($linkTarget, 'outside proof data must not be copied')
$linkPath = Join-Path $fixture 'verification/Linked.dfy'
$linkSupported = $false
try { [void][IO.File]::CreateSymbolicLink($linkPath, $linkTarget); $linkSupported = $true }
catch { "Symbolic-link creation unavailable on this host: $($_.Exception.Message)" }
if ($linkSupported) {
    try {
        Assert-Scenario 'failure snapshot refuses linked proof contents' {
            $output = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-linked-input-' + [guid]::NewGuid().ToString('N'))
            [void][IO.Directory]::CreateDirectory($output)
            Write-ProofFailureReceipt $fixture $output 'linked proof rejected'
            $failure = [IO.File]::ReadAllText((Join-Path $output 'failure.json')) | ConvertFrom-Json
            $input = @($failure.inputs | Where-Object path -CEQ 'verification/Linked.dfy')
            if ($input.Count -ne 1 -or $input[0].status -ne 'refused-linked' -or $input[0].sha256 -or
                [IO.File]::Exists((Join-Path $output 'inputs/verification/Linked.dfy'))) { throw 'Linked input was followed or refusal evidence was lost.' }
        }
    } finally { [IO.File]::Delete($linkPath) }
}
$linkedRoot = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-linked-root-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory((Join-Path $linkedRoot 'verification'))
[void][IO.Directory]::CreateDirectory((Join-Path $linkedRoot 'src/JetDatabaseWriter'))
$targetDirectory = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-linked-generated-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($targetDirectory)
[IO.File]::WriteAllText((Join-Path $targetDirectory 'RecoveryKernel.g.cs'), 'outside generated data must not be copied')
$directoryLink = Join-Path $linkedRoot 'src/JetDatabaseWriter/Verification'
$directoryLinkSupported = $false
try {
    if ($IsWindows) { New-Item -ItemType Junction -Path $directoryLink -Target $targetDirectory | Out-Null }
    else { [void][IO.Directory]::CreateSymbolicLink($directoryLink, $targetDirectory) }
    $directoryLinkSupported = $true
} catch { "Directory-link creation unavailable on this host: $($_.Exception.Message)" }
if ($directoryLinkSupported) {
    try {
        Assert-Scenario 'failure snapshot refuses a linked generated directory' {
            $output = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-linked-directory-' + [guid]::NewGuid().ToString('N'))
            [void][IO.Directory]::CreateDirectory($output)
            Write-ProofFailureReceipt $linkedRoot $output 'linked generated directory rejected'
            $failure = [IO.File]::ReadAllText((Join-Path $output 'failure.json')) | ConvertFrom-Json
            $input = @($failure.inputs | Where-Object path -CEQ 'src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs')
            if ($input.Count -ne 1 -or $input[0].status -ne 'refused-linked' -or
                [IO.File]::Exists((Join-Path $output 'inputs/src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs'))) { throw 'Linked input directory was followed.' }
        }
    } finally { [IO.Directory]::Delete($directoryLink) }
}
'Proof gate regression checks passed.'
