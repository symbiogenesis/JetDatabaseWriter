# Verifies the complete model inventory; no generated code enters the library.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $DafnyPath,
    [Parameter(Mandatory)][string] $OutputDirectory,
    [ValidateRange(10, 3600)][int] $TimeoutSeconds = 300
)
$ErrorActionPreference = 'Stop'

function Assert-ProofOutputDirectory([string] $Path, [string] $RepositoryRoot) {
    if ($Path -match '%[^%]+%') { throw 'The proof output path contains an unresolved environment variable.' }
    if (-not [IO.Path]::IsPathFullyQualified($Path)) { throw 'The proof output path must be absolute.' }
    $full = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetFullPath($RepositoryRoot).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    if ($full.Equals($root, [StringComparison]::OrdinalIgnoreCase) -or $full.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Proof logs and native working directories must be outside the repository.'
    }
    return $full
}

function Get-ProofFiles([string] $RepositoryRoot, [string] $ManifestPath) {
    $manifest = [IO.File]::ReadAllText($ManifestPath) | ConvertFrom-Json
    if ($manifest.schemaVersion -ne 1 -or @($manifest.files).Count -eq 0) { throw 'A schema version 1 nonempty proof inventory is required.' }
    $declared = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($file in $manifest.files) {
        if ($file -isnot [string] -or $file -notmatch '^verification/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.dfy$') { throw "Invalid proof path: $file" }
        if (-not $declared.Add($file)) { throw "Duplicate proof: $file" }
    }
    $actual = @(Get-ChildItem -LiteralPath $RepositoryRoot -Filter '*.dfy' -File -Recurse -Force | ForEach-Object { [IO.Path]::GetRelativePath($RepositoryRoot, $_.FullName).Replace('\', '/') })
    if ($actual.Count -eq 0) { throw 'Proof inventory mismatch: no proof sources found.' }
    $differences = @(Compare-Object -ReferenceObject @($declared | Sort-Object) -DifferenceObject @($actual | Sort-Object) -CaseSensitive)
    if ($differences.Count -gt 0) { throw ('Proof inventory mismatch: ' + (($differences | ForEach-Object { "$($_.SideIndicator) $($_.InputObject)" }) -join '; ')) }
    foreach ($file in $manifest.files) {
        $path = Join-Path $RepositoryRoot $file
        if ([IO.File]::GetAttributes($path) -band [IO.FileAttributes]::ReparsePoint) { throw "Linked proof files are not supported: $file" }
        $path
    }
}

function Get-DafnyPolicyText([string] $Source) {
    # Preserve token boundaries while discarding nested comments and string contents.
    # This is a conservative policy check; Dafny parsing and auditing remain required.
    $result = [Text.StringBuilder]::new()
    $index = 0
    while ($index -lt $Source.Length) {
        if ($index + 1 -lt $Source.Length -and $Source.Substring($index, 2) -eq '//') {
            while ($index -lt $Source.Length -and $Source[$index] -ne "`n") { $index++ }
            [void]$result.Append(' ')
        } elseif ($index + 1 -lt $Source.Length -and $Source.Substring($index, 2) -eq '/*') {
            $depth = 1
            $index += 2
            while ($index -lt $Source.Length -and $depth -gt 0) {
                if ($index + 1 -lt $Source.Length -and $Source.Substring($index, 2) -eq '/*') { $depth++; $index += 2 }
                elseif ($index + 1 -lt $Source.Length -and $Source.Substring($index, 2) -eq '*/') { $depth--; $index += 2 }
                else { $index++ }
            }
            [void]$result.Append(' ')
        } elseif ($Source[$index] -eq '"' -or ($Source[$index] -eq '@' -and $index + 1 -lt $Source.Length -and $Source[$index + 1] -eq '"')) {
            $verbatim = $Source[$index] -eq '@'
            if ($verbatim) { $index++ }
            $index++
            while ($index -lt $Source.Length) {
                if ($Source[$index] -eq '"') {
                    $index++
                    if ($verbatim -and $index -lt $Source.Length -and $Source[$index] -eq '"') { $index++; continue }
                    break
                }
                if (-not $verbatim -and $Source[$index] -eq '\') { $index++ }
                $index++
            }
            [void]$result.Append(' ')
        } else { [void]$result.Append($Source[$index]); $index++ }
    }
    return $result.ToString()
}

function Assert-ProofSource([string] $Path, [string] $Source) {
    $code = Get-DafnyPolicyText $Source
    if ($code -match '\b(include|assume|expect|extern|replaceable)\b|\bdecreases\s*\*|@') { throw "Disallowed proof construct in $Path" }
    foreach ($attribute in [regex]::Matches($code, '\{\s*:\s*([A-Za-z_][A-Za-z0-9_]*)')) {
        if ($attribute.Groups[1].Value -notin 'trigger', 'induction') { throw "Disallowed proof construct in ${Path}: $($attribute.Value)" }
    }
}

function Invoke-ProofProcess([string] $Executable, [string[]] $Arguments, [string] $WorkingDirectory, [string] $Name, [int] $TimeoutSeconds) {
    if (-not [IO.Path]::IsPathFullyQualified($Executable) -or -not [IO.Path]::IsPathFullyQualified($WorkingDirectory)) { throw 'Native proof process paths must be absolute.' }
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $Executable
    $start.WorkingDirectory = $WorkingDirectory
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { [void]$start.ArgumentList.Add($argument) }
    # Inherit every system/profile environment variable unchanged.
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $started = $false
    $stdout = $null
    $stderr = $null
    $failure = $null
    try {
        $started = $process.Start()
        if (-not $started) { throw "Cannot start proof process $Name" }
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) { throw "Proof process $Name timed out after $TimeoutSeconds seconds." }
        # Both pipes drain concurrently; inherited child pipe handles must never
        # turn an exited process into an infinite wait.
        foreach ($read in @($stdout, $stderr)) {
            if (-not ([IAsyncResult]$read).AsyncWaitHandle.WaitOne(5000)) { throw "Proof process $Name output drain timed out." }
        }
        return [pscustomobject]@{ ExitCode = $process.ExitCode; Stdout = $stdout.GetAwaiter().GetResult(); Stderr = $stderr.GetAwaiter().GetResult() }
    } catch {
        $failure = $_.Exception.Message
        throw
    } finally {
        try {
            if ($started -and -not $process.HasExited) {
                $process.Kill($true)
                if (-not $process.WaitForExit(5000)) { throw "Proof process $Name could not be stopped." }
            }
        } finally {
            foreach ($stream in @(@('stdout', $stdout), @('stderr', $stderr))) {
                $captured = '[Output capture did not complete.]'
                if ($null -ne $stream[1] -and ([IAsyncResult]$stream[1]).AsyncWaitHandle.WaitOne(1000) -and $stream[1].IsCompletedSuccessfully) { $captured = $stream[1].GetAwaiter().GetResult() }
                [IO.File]::WriteAllText((Join-Path $WorkingDirectory "$Name.$($stream[0]).log"), $captured)
            }
            if ($failure) { [IO.File]::WriteAllText((Join-Path $WorkingDirectory "$Name.failure.txt"), $failure) }
            $process.Dispose()
        }
    }
}

function Assert-ProofResult($Result, [string] $Name) {
    if ($Result.ExitCode -ne 0) { throw "Verification failed for $Name (exit $($Result.ExitCode)). $($Result.Stdout) $($Result.Stderr)" }
    if ($Result.Stdout -notmatch 'Dafny program verifier finished with ([1-9][0-9]*) verified, 0 errors\b') { throw "Verification requires a positive verification count and zero errors for $Name. $($Result.Stdout)" }
}

function Assert-ProofAudit($Result, [string] $ReportPath, [string] $Name) {
    if ($Result.ExitCode -ne 0) { throw "Audit failed for $Name (exit $($Result.ExitCode)). $($Result.Stdout) $($Result.Stderr)" }
    if (-not [IO.File]::Exists($ReportPath)) { throw "Missing audit report for $Name" }
    if ([IO.File]::ReadAllText($ReportPath).Length -ne 0 -or $Result.Stdout -notmatch 'Dafny auditor completed with 0 findings\b') { throw "Unreviewed audit findings for $Name. $([IO.File]::ReadAllText($ReportPath)) $($Result.Stdout)" }
}

function Invoke-ProofVerification([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [int] $TimeoutSeconds) {
    $output = Assert-ProofOutputDirectory $OutputDirectory $RepositoryRoot
    if (Test-Path -LiteralPath $output) { throw 'Proof output directory must not already exist; retain each run separately.' }
    [void][IO.Directory]::CreateDirectory($output)
    if (-not [IO.Path]::IsPathFullyQualified($DafnyPath)) { throw 'DafnyPath must be absolute.' }
    $executable = (Resolve-Path -LiteralPath $DafnyPath).Path
    $toolchain = [IO.File]::ReadAllText((Join-Path $RepositoryRoot 'verification/dafny-toolchain.json')) | ConvertFrom-Json
    if ($toolchain.schemaVersion -ne 1) { throw 'Unsupported Dafny toolchain manifest schema.' }
    $version = Invoke-ProofProcess $executable @('--version') $output 'version' 10
    if ($version.ExitCode -ne 0 -or $version.Stdout.Trim() -ne $toolchain.versionOutput) { throw "Dafny version does not match the pinned toolchain: $($version.Stdout)" }
    $solver = Join-Path (Split-Path $executable -Parent) $(if ($IsWindows) { $toolchain.solver.windows } else { $toolchain.solver.linux })
    $solverVersion = Invoke-ProofProcess $solver @('--version') $output 'solver-version' 10
    if ($solverVersion.ExitCode -ne 0 -or $solverVersion.Stdout.Trim() -ne $toolchain.solver.versionOutput) { throw "Z3 version does not match the pinned toolchain: $($solverVersion.Stdout)" }
    $files = @(Get-ProofFiles $RepositoryRoot (Join-Path $RepositoryRoot 'verification/proofs.json'))
    foreach ($file in $files) { Assert-ProofSource $file ([IO.File]::ReadAllText($file)) }
    $options = @('--cores', '1', '--verification-time-limit', '60', '--solver-path', $solver, '--verify-included-files')
    # Negative controls are mandatory: detect a disabled verifier and audit tools
    # which report success despite finding an assumption.
    $falsePath = Join-Path $output 'FalseTheorem.dfy'
    [IO.File]::WriteAllText($falsePath, 'lemma FalseTheorem() ensures false {}')
    $falseResult = Invoke-ProofProcess $executable (@('verify', $falsePath) + $options) $output 'negative-false' $TimeoutSeconds
    if ($falseResult.ExitCode -ne 4 -or $falseResult.Stdout -notmatch '[1-9][0-9]* errors?\b') { throw 'False-theorem negative control did not fail verification as expected.' }
    $assumePath = Join-Path $output 'AssumedTheorem.dfy'
    [IO.File]::WriteAllText($assumePath, 'lemma AssumedTheorem() ensures false { assume {:axiom} false; }')
    $assumeVerified = Invoke-ProofProcess $executable (@('verify', $assumePath) + $options) $output 'negative-assume.verify' $TimeoutSeconds
    Assert-ProofResult $assumeVerified 'assumption negative control'
    $assumeReport = Join-Path $output 'negative-assume.audit.txt'
    $assumeResult = Invoke-ProofProcess $executable @('audit', $assumePath, '--report-file', $assumeReport, '--report-format', 'txt') $output 'negative-assume' $TimeoutSeconds
    if ($assumeResult.ExitCode -ne 0 -or -not [IO.File]::Exists($assumeReport) -or [IO.File]::ReadAllText($assumeReport) -notmatch 'assume') { throw 'Assumption negative control did not produce the expected zero-exit audit finding.' }
    $auditRejected = $false
    try { Assert-ProofAudit $assumeResult $assumeReport 'assumption negative control' }
    catch {
        if (-not $_.Exception.Message.Contains('audit findings')) { throw }
        $auditRejected = $true
    }
    if (-not $auditRejected) { throw 'Assumption negative control was accepted by the audit policy.' }
    $summary = [Collections.Generic.List[object]]::new()
    foreach ($file in $files) {
        $name = [IO.Path]::GetFileNameWithoutExtension($file)
        $result = Invoke-ProofProcess $executable (@('verify', $file) + $options) $output "$name.verify" $TimeoutSeconds
        Assert-ProofResult $result $name
        $report = Join-Path $output "$name.audit.txt"
        $audit = Invoke-ProofProcess $executable @('audit', $file, '--report-file', $report, '--report-format', 'txt') $output "$name.audit" $TimeoutSeconds
        Assert-ProofAudit $audit $report $name
        $summary.Add([ordered]@{ file = [IO.Path]::GetRelativePath($RepositoryRoot, $file).Replace('\', '/'); sha256 = (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant(); verification = $result.Stdout.Trim(); auditFindings = 0 })
        Write-Output "Verified and audited: $name"
    }
    $receipt = [ordered]@{ schemaVersion = 1; dafny = $version.Stdout.Trim(); solver = $solverVersion.Stdout.Trim(); negativeControls = 'false theorem rejected; assumption audit detected'; proofs = @($summary.ToArray()) }
    [IO.File]::WriteAllText((Join-Path $output 'summary.json'), ($receipt | ConvertTo-Json -Depth 6))
    Write-Output "Proof models verified; C# implementation correspondence and storage assumptions remain separate obligations. Results: $output"
}

Invoke-ProofVerification (Split-Path $PSScriptRoot -Parent) $DafnyPath $OutputDirectory $TimeoutSeconds