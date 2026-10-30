# Review freshness and failure-evidence regressions use only synthetic files.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-proof-review-tests-' + [guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($fixture)
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile((Join-Path $repoRoot 'scripts/check-proof-review.ps1'), [ref]$null, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors.Message -join '; ') }
foreach ($definition in $ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
    . ([scriptblock]::Create($definition.Extent.Text))
}
function Write-Fixture([string] $Relative, [string] $Content) {
    $path = Join-Path $fixture $Relative
    [void][IO.Directory]::CreateDirectory((Split-Path $path -Parent))
    [IO.File]::WriteAllText($path, $Content)
    return $path
}
function Assert-Scenario([string] $Name, [scriptblock] $Action, [string] $ExpectedError) {
    $caught = $null
    try { & $Action | Out-Null } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or -not $caught.Contains($ExpectedError)) { throw "$Name expected '$ExpectedError', got '$caught'." }
    } elseif ($caught) { throw "$Name failed: $caught" }
    "PASS: $Name"
}
foreach ($path in @(Get-ProofReviewFixedInputs 'implementation') + @(Get-ProofReviewFixedInputs 'proof-trust')) {
    Write-Fixture $path "reviewed fixture $path`n" | Out-Null
}
Write-Fixture 'verification/Example.dfy' 'lemma Example() ensures true {}' | Out-Null
Write-Fixture 'src/JetDatabaseWriter/Verification/RecoveryKernel.g.cs' 'class RecoveryKernel {}' | Out-Null
Write-Fixture 'src/JetDatabaseWriter/Unmapped.cs' 'class Unmapped {}' | Out-Null
Write-Fixture 'scripts/ci-matrix.ps1' 'function Get-CiAnalyzerMatrix {}' | Out-Null
function New-Receipt {
    $reviews = foreach ($boundary in 'implementation', 'proof-trust') {
        $inputs = foreach ($path in Get-ProofReviewInputs $fixture $boundary) {
            [ordered]@{ path = $path; sha256 = Get-ProofReviewHash (Join-Path $fixture $path) }
        }
        [ordered]@{
            boundary = $boundary
            obligations = 'Inspect source effects and every changed proof or trust boundary.'
            conformance = 'Check selected production behavior and mandatory verification controls.'
            remainingAssumptions = 'Operating system effects and reviewer judgment remain explicit assumptions.'
            evidence = @('docs/design/formal-verification.md')
            inputs = @($inputs)
        }
    }
    return [ordered]@{ schemaVersion = 1; productionInventorySha256 = Get-ProofReviewInventoryHash $fixture; inventoryReview = 'All production source names were classified against the mapped boundary.'; reviews = @($reviews) }
}
$receipt = New-Receipt
$reviewPath = Write-Fixture 'verification/proof-review.json' ($receipt | ConvertTo-Json -Depth 8)
function Save-Receipt($Value) { [IO.File]::WriteAllText($reviewPath, ($Value | ConvertTo-Json -Depth 8)) }
Assert-Scenario 'complete reviewed source inventory' { Assert-ProofReview $fixture $reviewPath }
Assert-Scenario 'mapped source edit requires review' {
    $path = Join-Path $fixture 'src/JetDatabaseWriter/Pages/Paging/PersistentRollbackJournal.cs'
    $original = [IO.File]::ReadAllText($path)
    try { [IO.File]::WriteAllText($path, 'changed acquisition'); Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::WriteAllText($path, $original) }
} 'Stale proof review'
Assert-Scenario 'pin update requires trust review' {
    $path = Join-Path $fixture 'verification/dafny-toolchain.json'
    $original = [IO.File]::ReadAllText($path)
    try { [IO.File]::WriteAllText($path, 'changed verifier pin'); Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::WriteAllText($path, $original) }
} 'Stale proof review'
Assert-Scenario 'CI matrix update requires trust review' {
    $path = Join-Path $fixture 'scripts/ci-matrix.ps1'
    $original = [IO.File]::ReadAllText($path)
    try { [IO.File]::WriteAllText($path, 'function Get-CiAnalyzerMatrix { @() }'); Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::WriteAllText($path, $original) }
} 'Stale proof review'
Assert-Scenario 'proof edit requires review' {
    $path = Join-Path $fixture 'verification/Example.dfy'
    try { [IO.File]::WriteAllText($path, 'lemma Example() ensures false {}'); Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::WriteAllText($path, 'lemma Example() ensures true {}') }
} 'Stale proof review'
Assert-Scenario 'new unmapped production source needs classification' {
    $path = Write-Fixture 'src/JetDatabaseWriter/NewSurface.cs' 'class NewSurface {}'
    try { Assert-ProofReview $fixture $reviewPath } finally { [IO.File]::Delete($path) }
} 'Production source inventory'
Assert-Scenario 'hidden production source needs classification' {
    $path = Write-Fixture 'src/JetDatabaseWriter/.HiddenSurface.cs' 'class HiddenSurface {}'
    if ($IsWindows) { [IO.File]::SetAttributes($path, [IO.File]::GetAttributes($path) -bor [IO.FileAttributes]::Hidden) }
    try { Assert-ProofReview $fixture $reviewPath } finally { [IO.File]::Delete($path) }
} 'Production source inventory'
Assert-Scenario 'hidden proof cannot be omitted' {
    $path = Write-Fixture 'verification/.HiddenModel.dfy' 'lemma HiddenModel() {}'
    if ($IsWindows) { [IO.File]::SetAttributes($path, [IO.File]::GetAttributes($path) -bor [IO.FileAttributes]::Hidden) }
    try { Assert-ProofReview $fixture $reviewPath } finally { [IO.File]::Delete($path) }
} 'Proof review input inventory'
Assert-Scenario 'renamed production source needs classification' {
    $path = Join-Path $fixture 'src/JetDatabaseWriter/Unmapped.cs'
    $moved = Join-Path $fixture 'src/JetDatabaseWriter/Renamed.cs'
    try { [IO.File]::Move($path, $moved); Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::Move($moved, $path) }
} 'Production source inventory'
Assert-Scenario 'deleted production source needs classification' {
    $path = Join-Path $fixture 'src/JetDatabaseWriter/Unmapped.cs'
    try { [IO.File]::Delete($path); Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::WriteAllText($path, 'class Unmapped {}') }
} 'Production source inventory'
Assert-Scenario 'new kernel caller cannot be accepted by inventory hash alone' {
    $path = Write-Fixture 'src/JetDatabaseWriter/NewCaller.cs' 'class NewCaller { bool X() => RecoveryKernel.Accept(0); }'
    $changed = New-Receipt
    $changed.reviews = $receipt.reviews
    Save-Receipt $changed
    try { Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::Delete($path); Save-Receipt $receipt }
} 'Proof review input inventory'
foreach ($adapter in 'BinaryBuffer', 'StreamRead') {
    Assert-Scenario "new $adapter caller requires mapped review" {
        $path = Write-Fixture 'src/JetDatabaseWriter/NewAdapterCaller.cs' "class NewAdapterCaller { object X() => $adapter.Read(null); }"
        $changed = New-Receipt
        $changed.reviews = $receipt.reviews
        Save-Receipt $changed
        try { Assert-ProofReview $fixture $reviewPath }
        finally { [IO.File]::Delete($path); Save-Receipt $receipt }
    } 'Proof review input inventory'
}
Assert-Scenario 'new proof cannot be omitted' {
    $path = Write-Fixture 'verification/NewModel.dfy' 'lemma NewModel() {}'
    try { Assert-ProofReview $fixture $reviewPath } finally { [IO.File]::Delete($path) }
} 'Proof review input inventory'
Assert-Scenario 'unrelated source content does not invalidate the boundary review' {
    $path = Join-Path $fixture 'src/JetDatabaseWriter/Unmapped.cs'
    try { [IO.File]::WriteAllText($path, 'class Unmapped { int Value => 1; }'); Assert-ProofReview $fixture $reviewPath }
    finally { [IO.File]::WriteAllText($path, 'class Unmapped {}') }
}
Assert-Scenario 'line endings do not change reviewed content identity' {
    $path = Write-Fixture 'line-endings.txt' "a`r`nb`r`n"
    $hash = Get-ProofReviewHash $path
    [IO.File]::WriteAllText($path, "a`nb`n")
    if ($hash -ne (Get-ProofReviewHash $path)) { throw 'Line ending normalization differs.' }
}
foreach ($property in 'obligations', 'conformance', 'remainingAssumptions') {
    Assert-Scenario "empty $property is not review evidence" {
        $changed = New-Receipt
        $changed.reviews[0][$property] = ' '
        Save-Receipt $changed
        try { Assert-ProofReview $fixture $reviewPath } finally { Save-Receipt $receipt }
    } 'Missing proof review explanation'
}
Assert-Scenario 'duplicate input cannot mask an omitted input' {
    $changed = New-Receipt
    $changed.reviews[0].inputs += $changed.reviews[0].inputs[0]
    Save-Receipt $changed
    try { Assert-ProofReview $fixture $reviewPath } finally { Save-Receipt $receipt }
} 'Duplicate proof review input'
Assert-Scenario 'unknown review boundary rejected' {
    $changed = New-Receipt
    $changed.reviews[0].boundary = 'other'
    Save-Receipt $changed
    try { Assert-ProofReview $fixture $reviewPath } finally { Save-Receipt $receipt }
} 'Proof review boundaries'
Assert-Scenario 'missing evidence path rejected' {
    $changed = New-Receipt
    $changed.reviews[0].evidence = @('docs/missing.md')
    Save-Receipt $changed
    try { Assert-ProofReview $fixture $reviewPath } finally { Save-Receipt $receipt }
} 'Missing proof review evidence'
Assert-Scenario 'evidence path cannot escape repository' {
    $changed = New-Receipt
    $changed.reviews[0].evidence = @('../outside.md')
    Save-Receipt $changed
    try { Assert-ProofReview $fixture $reviewPath } finally { Save-Receipt $receipt }
} 'Invalid proof review path'
Assert-Scenario 'review check retains a failure report' {
    $output = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-review-result-' + [guid]::NewGuid().ToString('N'))
    $changed = New-Receipt
    $changed.productionInventorySha256 = '0' * 64
    Save-Receipt $changed
    try {
        try { Invoke-ProofReview $fixture $reviewPath $output } catch { }
        $report = [IO.File]::ReadAllText((Join-Path $output 'review.json')) | ConvertFrom-Json
        if ($report.status -ne 'failed' -or $report.error -notmatch 'Production source inventory') { throw 'Review failure was lost.' }
    } finally { Save-Receipt $receipt }
}
Assert-Scenario 'relative review output refused' { Invoke-ProofReview $fixture $reviewPath 'relative/output' } 'absolute'
Assert-Scenario 'in-repository review output refused' { Invoke-ProofReview $fixture $reviewPath (Join-Path $fixture 'logs') } 'outside'
'Proof review regression checks passed.'
