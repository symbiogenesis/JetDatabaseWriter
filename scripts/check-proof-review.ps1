# Enforce fresh, explicit review records for mapped implementation and proof trust.
[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'

function Get-ProofReviewFixedInputs([string] $Boundary) {
    switch -Exact ($Boundary) {
        'implementation' {
            @(
                'src/JetDatabaseWriter/AccessWriter.cs'
                'src/JetDatabaseWriter/JetTransaction.cs'
                'src/JetDatabaseWriter/Transactions/TransactionLifecycle.cs'
                'src/JetDatabaseWriter/Transactions/RecoverableWriterLock.cs'
                'src/JetDatabaseWriter/Transactions/LockFileCoordinator.cs'
                'src/JetDatabaseWriter/Pages/Paging/PagerTransaction.cs'
                'src/JetDatabaseWriter/Pages/Paging/Pager.cs'
                'src/JetDatabaseWriter/Pages/Paging/PhysicalRollback.cs'
                'src/JetDatabaseWriter/Pages/Paging/PersistentRollbackJournal.cs'
                'src/JetDatabaseWriter/Pages/Paging/StreamPageStore.cs'
                'src/JetDatabaseWriter/Infrastructure/StreamRead.cs'
                'src/JetDatabaseWriter/Infrastructure/BinaryBuffer.cs'
                'src/JetDatabaseWriter/LongValues/LongValueStore.cs'
                'src/JetDatabaseWriter/LongValues/Models/LongValueDescriptor.cs'
                'src/JetDatabaseWriter/Encryption/EncryptionReplacementIntent.cs'
                'docs/design/formal-verification.md'
            )
        }
        'proof-trust' {
            @(
                'verification/dafny-toolchain.json'
                'verification/proofs.json'
                'verification/README.md'
                'global.json'
                '.github/dotnet11/global.json'
                'Directory.Build.props'
                'src/JetDatabaseWriter/JetDatabaseWriter.csproj'
                '.github/workflows/ci.yml'
                '.github/ci-tools.slnf'
                'scripts/select-ci-lanes.ps1'
                'scripts/tests/select-ci-lanes.tests.ps1'
                'scripts/tests/ci-tools.tests.ps1'
                'scripts/tests/ci-workflow.tests.ps1'
                'scripts/tests/test-scripts.tests.ps1'
                'tests/JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj'
                'tests/JetDatabaseWriter.Tests/TestProgram.cs'
                'tests/JetDatabaseWriter.Tests/Infrastructure/CrashRecoveryProcess.cs'
                'tests/JetDatabaseWriter.Tests/Infrastructure/CrashRecoveryProcessTests.cs'
                'scripts/install-dafny.ps1'
                'scripts/verify-proofs.ps1'
                'scripts/check-proof-review.ps1'
                'scripts/test-scripts.ps1'
                'scripts/tests/verify-proofs.tests.ps1'
                'scripts/tests/proof-review.tests.ps1'
                'scripts/tests/formal-workflow.tests.ps1'
                'docs/design/formal-verification.md'
                'docs/design/recovery-storage-contract.md'
                'docs/design/proof-trust-review.md'
            )
        }
        default { throw "Unknown proof review boundary: $Boundary" }
    }
}

function Get-ProofReviewSourcePaths([string] $RepositoryRoot) {
    $sourceRoot = Join-Path $RepositoryRoot 'src/JetDatabaseWriter'
    $paths = @(Get-ChildItem -LiteralPath $sourceRoot -Filter '*.cs' -File -Recurse -Force | ForEach-Object {
        $relative = [IO.Path]::GetRelativePath($RepositoryRoot, $_.FullName).Replace('\', '/')
        if ($relative -notmatch '/(?:bin|obj)/') {
            if ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Linked proof review input: $relative" }
            $relative
        }
    })
    [Array]::Sort($paths, [StringComparer]::Ordinal)
    return $paths
}

function Get-ProofReviewTextHash([string] $Text) {
    $bytes = [Text.Encoding]::UTF8.GetBytes($Text.Replace("`r`n", "`n"))
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try { return [Convert]::ToHexString($algorithm.ComputeHash($bytes)).ToLowerInvariant() }
    finally { $algorithm.Dispose() }
}

function Get-ProofReviewHash([string] $Path) {
    if (-not [IO.File]::Exists($Path)) { throw "Missing proof review input: $Path" }
    if ([IO.File]::GetAttributes($Path) -band [IO.FileAttributes]::ReparsePoint) { throw "Linked proof review input: $Path" }
    return Get-ProofReviewTextHash ([IO.File]::ReadAllText($Path))
}

function Get-ProofReviewInventoryHash([string] $RepositoryRoot) {
    return Get-ProofReviewTextHash ((@(Get-ProofReviewSourcePaths $RepositoryRoot) -join "`n") + "`n")
}

function Get-ProofReviewInputs([string] $RepositoryRoot, [string] $Boundary) {
    $paths = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($path in Get-ProofReviewFixedInputs $Boundary) { [void]$paths.Add($path) }
    if ($Boundary -ceq 'implementation') {
        foreach ($path in Get-ProofReviewSourcePaths $RepositoryRoot) {
            if ($path.StartsWith('src/JetDatabaseWriter/Verification/', [StringComparison]::Ordinal) -or
                [IO.File]::ReadAllText((Join-Path $RepositoryRoot $path)) -match '\b(?:RecoveryKernel|LongValueKernel|BinaryBuffer|StreamRead)\s*\.') {
                [void]$paths.Add($path)
            }
        }
    } else {
        foreach ($file in Get-ChildItem -LiteralPath (Join-Path $RepositoryRoot 'verification') -Filter '*.dfy' -File -Recurse -Force) {
            [void]$paths.Add([IO.Path]::GetRelativePath($RepositoryRoot, $file.FullName).Replace('\', '/'))
        }
    }
    $sorted = @($paths)
    [Array]::Sort($sorted, [StringComparer]::Ordinal)
    return $sorted
}

function Assert-ProofReviewPath([string] $Path) {
    if ([string]::IsNullOrWhiteSpace($Path) -or $Path -match '(^/|\\|:|(?:^|/)\.\.?(/|$))' -or [IO.Path]::IsPathRooted($Path)) {
        throw "Invalid proof review path: $Path"
    }
}

function Assert-ProofReview([string] $RepositoryRoot, [string] $ReviewPath) {
    $receipt = [IO.File]::ReadAllText($ReviewPath) | ConvertFrom-Json
    if ($receipt.schemaVersion -ne 1) { throw 'Unsupported proof review schema.' }
    if ([string]::IsNullOrWhiteSpace($receipt.inventoryReview)) { throw 'Missing proof review explanation: inventoryReview' }
    if ($receipt.productionInventorySha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $receipt.productionInventorySha256 -cne (Get-ProofReviewInventoryHash $RepositoryRoot)) {
        throw 'Production source inventory changed; classify added, moved or deleted sources and review the boundary mapping.'
    }
    $reviews = @($receipt.reviews)
    if ($reviews.Count -ne 2 -or (@($reviews.boundary | Sort-Object -CaseSensitive) -join ',') -cne 'implementation,proof-trust') {
        throw 'Proof review boundaries must be exactly implementation and proof-trust.'
    }
    foreach ($review in $reviews) {
        foreach ($field in 'obligations', 'conformance', 'remainingAssumptions') {
            if ($review.$field -isnot [string] -or [string]::IsNullOrWhiteSpace($review.$field)) { throw "Missing proof review explanation: $($review.boundary).$field" }
        }
        $expected = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($path in Get-ProofReviewInputs $RepositoryRoot $review.boundary) { [void]$expected.Add($path) }
        if (@($review.evidence).Count -eq 0) { throw 'Missing proof review evidence.' }
        foreach ($path in $review.evidence) {
            Assert-ProofReviewPath $path
            if (-not [IO.File]::Exists((Join-Path $RepositoryRoot $path))) { throw "Missing proof review evidence: $path" }
            [void]$expected.Add($path)
        }
        $actual = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($input in $review.inputs) {
            Assert-ProofReviewPath $input.path
            if (-not $actual.Add($input.path)) { throw "Duplicate proof review input: $($input.path)" }
        }
        if (-not $expected.SetEquals($actual)) { throw "Proof review input inventory differs for $($review.boundary); review every required source and cited evidence file." }
        foreach ($input in $review.inputs) {
            if ($input.sha256 -cnotmatch '^[0-9a-f]{64}$' -or $input.sha256 -cne (Get-ProofReviewHash (Join-Path $RepositoryRoot $input.path))) {
                throw "Stale proof review: $($input.path). Review its contracts, production correspondence, assumptions and trust impact before recording the new hash."
            }
        }
    }
    return [ordered]@{ status = 'passed'; sourceInventory = $receipt.productionInventorySha256; boundaries = @($reviews.boundary); claim = 'Review-record freshness only; reviewer judgment, C# refinement and physical durability are not established by hashes.' }
}

function Invoke-ProofReview([string] $RepositoryRoot, [string] $ReviewPath, [string] $OutputDirectory) {
    if (-not [IO.Path]::IsPathFullyQualified($OutputDirectory)) { throw 'Proof review output must be absolute.' }
    $output = [IO.Path]::GetFullPath($OutputDirectory)
    $root = [IO.Path]::GetFullPath($RepositoryRoot).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    if ($output.Equals($root, [StringComparison]::OrdinalIgnoreCase) -or $output.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Proof review output must be outside the repository.'
    }
    if ([IO.Directory]::Exists($OutputDirectory)) { throw 'Proof review output must not already exist; preserve each review run.' }
    [void][IO.Directory]::CreateDirectory($OutputDirectory)
    try {
        $report = Assert-ProofReview $RepositoryRoot $ReviewPath
        [IO.File]::WriteAllText((Join-Path $OutputDirectory 'review.json'), ($report | ConvertTo-Json -Depth 4))
        Write-Output 'Proof/trust review records match the exact mapped sources, proof inputs and pins.'
    } catch {
        $report = [ordered]@{ status = 'failed'; error = $_.Exception.Message; claim = 'No fresh review receipt was established.' }
        [IO.File]::WriteAllText((Join-Path $OutputDirectory 'review.json'), ($report | ConvertTo-Json -Depth 4))
        throw
    }
}

$repositoryRoot = Split-Path $PSScriptRoot -Parent
Invoke-ProofReview $repositoryRoot (Join-Path $repositoryRoot 'verification/proof-review.json') $OutputDirectory
