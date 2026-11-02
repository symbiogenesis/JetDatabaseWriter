# Verifies the complete proof inventory and checks the shipped recovery kernel.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $DafnyPath,
    [Parameter(Mandatory)][string] $OutputDirectory,
    [ValidateRange(10, 3600)][int] $TimeoutSeconds = 300,
    [switch] $UpdateGeneratedSource,
    [switch] $AnalyzeProofCoverage,
    [ValidateNotNullOrEmpty()][string[]] $ProofCoverageSources,
    [switch] $SuggestProofRefactoring,
    [switch] $MeasureProofComplexity,
    [ValidateNotNullOrEmpty()][string[]] $ProofComplexitySources,
    [ValidateRange(2, 20)][int] $ProofComplexityMutations = 3,
    [string] $ProofDiagnosticPosition
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

function Get-ProofDependencies([string] $RepositoryRoot, [string] $ManifestPath, [string[]] $ProofFiles) {
    $manifest = [IO.File]::ReadAllText($ManifestPath) | ConvertFrom-Json
    $result = @{}
    if (-not $manifest.PSObject.Properties['dependencies']) { return $result }
    if ($manifest.dependencies -isnot [pscustomobject]) { throw 'Invalid proof dependency map.' }
    $earlier = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $known = @($ProofFiles | ForEach-Object { [IO.Path]::GetRelativePath($RepositoryRoot, $_).Replace('\', '/') })
    foreach ($property in $manifest.dependencies.PSObject.Properties) {
        if ($property.Name -cnotin $known) { throw "Invalid proof dependency owner: $($property.Name)" }
    }
    foreach ($file in $ProofFiles) {
        $relative = [IO.Path]::GetRelativePath($RepositoryRoot, $file).Replace('\', '/')
        $property = $manifest.dependencies.PSObject.Properties[$relative]
        $dependencies = [Collections.Generic.List[string]]::new()
        $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        if ($property) {
            if ($property.Value -isnot [array] -or $property.Value.Count -eq 0) { throw "Invalid proof dependency list: $relative" }
            foreach ($dependency in $property.Value) {
                # Dependencies are normal verified inputs, never trusted --library
                # contracts. Require earlier audited members and forbid cycles.
                if ($dependency -isnot [string] -or -not $earlier.Contains($dependency) -or -not $seen.Add($dependency)) { throw "Invalid proof dependency dependency: $relative -> $dependency" }
                $dependencies.Add((Join-Path $RepositoryRoot $dependency))
            }
        }
        $result[$file] = $dependencies.ToArray()
        [void]$earlier.Add($relative)
    }
    return $result
}

function Get-NativeKernelNames {
    return 'RecoveryKernel', 'LongValueKernel', 'DataPageKernel', 'IndexMutationKernel', 'UsageMapKernel', 'PageDigestKernel'
}

function Assert-NativeKernelName([string] $KernelName) {
    if ($KernelName -cnotin (Get-NativeKernelNames)) { throw "Unsupported native kernel: $KernelName" }
}

function Get-NativeKernelSource([string] $KernelName) {
    Assert-NativeKernelName $KernelName
    switch -Exact ($KernelName) {
        'RecoveryKernel' { return 'verification/Pages/Paging/RecoveryKernel.dfy' }
        'LongValueKernel' { return 'verification/LongValues/LongValueKernel.dfy' }
        'DataPageKernel' { return 'verification/Pages/DataPageKernel.dfy' }
        'IndexMutationKernel' { return 'verification/Indexes/IndexMutationKernel.dfy' }
        'UsageMapKernel' { return 'verification/Pages/UsageMapKernel.dfy' }
        'PageDigestKernel' { return 'verification/Pages/Paging/PageDigestKernel.dfy' }
    }
}

function ConvertFrom-NativeKernelTranslation([string] $KernelName, [string] $Translation) {
    Assert-NativeKernelName $KernelName
    $text = $Translation.Replace("`r`n", "`n")
    $header = "namespace JetDatabaseWriter.Verification.$KernelName {`n`n  public partial class __default {`n"
    $footer = "`n} // end of namespace JetDatabaseWriter.Verification.$KernelName`nnamespace JetDatabaseWriter.Verification {`n`n} // end of namespace JetDatabaseWriter.Verification"
    # The compiler prelude embeds the Dafny source as a string. Select the final
    # compiled namespace, never a matching string inside that source attribute.
    $start = $text.LastIndexOf($header, [StringComparison]::Ordinal)
    $end = $text.LastIndexOf($footer, [StringComparison]::Ordinal)
    if ($start -lt 0 -or $end -le $start -or $text.Substring($end + $footer.Length).Trim().Length -ne 0) { throw 'Unsupported native kernel translation structure.' }
    $body = $text.Substring($start + $header.Length, $end - $start - $header.Length)
    if (-not $body.EndsWith("  }") -or $body -match '\b(namespace|class|struct|interface)\b') { throw 'Unsupported native kernel translation body.' }
    if ($body -match '\b(Dafny|BigInteger|System|FuncExtensions|UndoJournal|NativeRecoveryTypes|LongValueNativeTypes|DataPageNativeTypes|IndexMutationNativeTypes|UsageMapNativeTypes|PageDigestNativeTypes)\b') { throw 'The native kernel requires unreviewed runtime support.' }
    if ($KernelName -cin 'LongValueKernel', 'UsageMapKernel') {
        if ($body -match '\b(MethodImpl(?:Attribute|Options)?|AggressiveInlining)\b') { throw 'Unreviewed native-kernel metadata.' }
        # The raw body has already passed the unchanged runtime-support check.
        # Only these exact compiler declarations receive fixed BCL metadata;
        # no input attribute or configurable method/option is admitted.
        $attribute = '    [global::System.Runtime.CompilerServices.MethodImplAttribute(global::System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]' + "`n"
        $controllers = if ($KernelName -ceq 'LongValueKernel') {
            @(
                @{ Name = 'PrepareVisitedAdd'; Declaration = '    public static ulong PrepareVisitedAdd(uint[] entries, uint count, uint pointer)' },
                @{ Name = 'CompleteVisitedGrowth'; Declaration = '    public static uint CompleteVisitedGrowth(uint[] source, uint[] destination, uint count, uint pointer)' }
            )
        } else {
            @(@{ Name = 'NextBitmapPage'; Declaration = '    public static void NextBitmapPage(byte[] bytes, uint offset, uint count, long basePage, long total, long minimum, bool strict, uint cursor, out uint next, out byte members, out byte state, out long firstPage)' })
        }
        foreach ($controller in $controllers) {
            $declarationPattern = '(?m)^[ \t]*(?:public|private|protected|internal|static)\b[^;{}]*?\b' + [regex]::Escape($controller.Name) + '\s*(?:<[^;{}]*>)?\s*\('
            $anchorPattern = '(?m)^' + [regex]::Escape($controller.Declaration + "`n    {`n")
            $anchors = [regex]::Matches($body, $anchorPattern)
            if ([regex]::Matches($body, $declarationPattern).Count -ne 1 -or $anchors.Count -ne 1) {
                throw "Unsupported native controller declaration: $($controller.Name)"
            }
            $body = $body.Insert($anchors[0].Index, $attribute)
        }
    }
    # Namespace/type packaging and the reviewed controller metadata lines
    # change. Generated method bodies and native integer operations are retained,
    # including generated self references to the renamed type.
    $body = $body.Replace("JetDatabaseWriter.Verification.$KernelName.__default", "JetDatabaseWriter.Verification.$KernelName")
    $body = [regex]::Replace($body, '\b__default\b', $KernelName)
    $projected = "// <auto-generated/>`n// Generated from $(Get-NativeKernelSource $KernelName) by scripts/verify-proofs.ps1.`n// Method bodies are emitted by pinned Dafny; do not edit.`nnamespace JetDatabaseWriter.Verification {`n`n  internal static class $KernelName {`n" + $body + "`n}`n"
    return $projected.Replace("`n", "`r`n")
}

function Sync-NativeKernel([string] $KernelName, [string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [string[]] $Dependencies, [string[]] $Options, [int] $TimeoutSeconds, [bool] $Update) {
    Assert-NativeKernelName $KernelName
    $relativeSource = Get-NativeKernelSource $KernelName
    $source = Join-Path $RepositoryRoot $relativeSource
    $translated = Join-Path $OutputDirectory "$KernelName.cs"
    $arguments = @('translate', 'cs') + $Dependencies + @($source, '--output', $translated, '--outer-module', 'JetDatabaseWriter.Verification', '--use-basename-for-filename', '--enforce-determinism') + $Options
    $result = Invoke-ProofProcess $DafnyPath $arguments $OutputDirectory "$KernelName.translate" $TimeoutSeconds
    Assert-ProofResult $result "$KernelName translation"
    $projected = ConvertFrom-NativeKernelTranslation $KernelName ([IO.File]::ReadAllText($translated))
    $destination = Join-Path $RepositoryRoot "src/JetDatabaseWriter/Verification/$KernelName.g.cs"
    if ($Update) {
        [void][IO.Directory]::CreateDirectory((Split-Path $destination -Parent))
        [IO.File]::WriteAllText($destination, $projected)
    } elseif (-not [IO.File]::Exists($destination) -or [IO.File]::ReadAllText($destination) -cne $projected) {
        throw "Generated $KernelName is stale. Review the proof and regenerate with -UpdateGeneratedSource."
    }
    return [ordered]@{ source = $relativeSource; generated = "src/JetDatabaseWriter/Verification/$KernelName.g.cs"; sha256 = (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant(); translation = $result.Stdout.Trim() }
}

function Get-DafnyPolicyText([string] $Source, [switch] $PreserveStrings) {
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
            $stringStart = $index
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
            if ($PreserveStrings) { [void]$result.Append($Source.Substring($stringStart, $index - $stringStart)) }
            else { [void]$result.Append(' ') }
        } else { [void]$result.Append($Source[$index]); $index++ }
    }
    return $result.ToString()
}

function Assert-ProofSource([string] $Path, [string] $Source) {
    $code = Get-DafnyPolicyText $Source
    if ($code -match '\b(include|assume|expect|extern|replaceable)\b|\bdecreases\s*\*|@') { throw "Disallowed proof construct in $Path" }
    foreach ($attribute in [regex]::Matches($code, '\{\s*:\s*([A-Za-z_][A-Za-z0-9_]*)')) {
        if ($attribute.Groups[1].Value -cnotin 'trigger', 'induction', 'nativeType', 'isolate') { throw "Disallowed proof construct in ${Path}: $($attribute.Value)" }
    }
    # These native representations affect compilation, not proof trust. Keep the
    # reviewed set exact; never accept nativeType overrides or arbitrary attributes.
    $withStrings = Get-DafnyPolicyText $Source -PreserveStrings
    foreach ($attribute in [regex]::Matches($withStrings, '\{\s*:\s*nativeType\b(?<value>[^}]*)\}')) {
        if ($attribute.Groups['value'].Value.Trim() -cnotmatch '^"(?:ulong|uint|byte|int|long)"$') { throw "Disallowed proof construct in ${Path}: unreviewed nativeType" }
    }
    foreach ($attribute in [regex]::Matches($withStrings, '\{\s*:\s*isolate\b(?<value>[^}]*)\}')) {
        if ($attribute.Groups['value'].Value.Trim() -cnotmatch '^(?:"paths")?$') { throw "Disallowed proof construct in ${Path}: unreviewed isolate argument" }
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
            $outcome = [ordered]@{
                name = $Name
                executable = $Executable
                arguments = @($Arguments)
                started = $started
                exitCode = $(if ($started -and $process.HasExited) { $process.ExitCode } else { $null })
                failure = $failure
            }
            [IO.File]::WriteAllText((Join-Path $WorkingDirectory "$Name.process.json"), ($outcome | ConvertTo-Json -Depth 4))
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

function Invoke-ProofInventory([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [string[]] $Files, [string[]] $Options, [int] $TimeoutSeconds) {
    # Supply every inventoried source as an ordinary verified input together.
    # This checks shared dependencies once rather than re-proving their bodies
    # separately for every importing module. Translation still verifies each
    # native kernel and its declared normal-source dependencies independently.
    $resourceLog = Join-Path $OutputDirectory 'inventory.resources.csv'
    $result = Invoke-ProofProcess $DafnyPath (@('verify') + $Files + $Options + @('--log-format', "csv;LogFileName=$resourceLog")) $OutputDirectory 'inventory.verify' $TimeoutSeconds
    Assert-ProofResult $result 'complete inventory'
    if (-not [IO.File]::Exists($resourceLog) -or (Get-Item -LiteralPath $resourceLog).Length -eq 0) { throw 'Missing inventory resource log.' }
    $report = Join-Path $OutputDirectory 'inventory.audit.txt'
    $audit = Invoke-ProofProcess $DafnyPath (@('audit') + $Files + @('--report-file', $report, '--report-format', 'txt')) $OutputDirectory 'inventory.audit' $TimeoutSeconds
    Assert-ProofAudit $audit $report 'complete inventory'
    foreach ($file in $Files) {
        [ordered]@{
            file = [IO.Path]::GetRelativePath($RepositoryRoot, $file).Replace('\', '/')
            sha256 = (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant()
            verification = $result.Stdout.Trim()
            verificationLog = 'inventory.verify.stdout.log'
            resourceLog = 'inventory.resources.csv'
            auditFindings = 0
        }
    }
}

function Get-ProofCoverageFiles([string] $RepositoryRoot, [string[]] $Files, $Dependencies, [string[]] $RequestedSources = $null) {
    if ($null -eq $RequestedSources) { return $Files }
    if ($RequestedSources.Count -eq 0) { throw 'ProofCoverageSources must contain at least one admitted proof source.' }
    $byRelativePath = [Collections.Generic.Dictionary[string,string]]::new([StringComparer]::Ordinal)
    foreach ($file in $Files) { $byRelativePath.Add([IO.Path]::GetRelativePath($RepositoryRoot, $file).Replace('\', '/'), $file) }
    $requested = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $pending = [Collections.Generic.Stack[string]]::new()
    foreach ($source in $RequestedSources) {
        if ($source -cnotmatch '^verification/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.dfy$' -or -not $byRelativePath.ContainsKey($source)) {
            throw "Invalid or unknown proof coverage source: $source"
        }
        if (-not $requested.Add($source)) { throw "Duplicate proof coverage source: $source" }
        $pending.Push($byRelativePath[$source])
    }
    $closure = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    while ($pending.Count -gt 0) {
        $file = $pending.Pop()
        if (-not $closure.Add($file)) { continue }
        foreach ($dependency in $Dependencies[$file]) { $pending.Push($dependency) }
    }
    # Dependencies are the already-validated normal-source manifest entries.
    # Preserve their manifest order, independently of the requested root order.
    return @($Files | Where-Object { $closure.Contains($_) })
}

function Get-ProofDiagnosticPosition([string] $RepositoryRoot, [string[]] $RequestedSources, [string] $Position) {
    if ([string]::IsNullOrEmpty($Position)) { return $null }
    if ($RequestedSources.Count -ne 1) { throw 'A diagnostic position requires one explicitly selected source.' }
    $match = [regex]::Match($Position, '^(?:(?<start>[1-9][0-9]*)(?:-(?<end>[1-9][0-9]*)?)?|-(?<end>[1-9][0-9]*))$')
    $start = 0
    $end = 0
    if (-not $match.Success -or
        ($match.Groups['start'].Success -and -not [int]::TryParse($match.Groups['start'].Value, [ref]$start)) -or
        ($match.Groups['end'].Success -and -not [int]::TryParse($match.Groups['end'].Value, [ref]$end)) -or
        ($start -gt 0 -and $end -gt 0 -and $end -lt $start)) { throw "Invalid diagnostic position: $Position" }
    # Dafny matches normalized source paths even when the input uses Windows paths.
    return (Join-Path $RepositoryRoot $RequestedSources[0]).Replace('\', '/') + ':' + $Position
}

function Assert-ProofInputsUnchanged([string] $RepositoryRoot, [object[]] $Proofs) {
    foreach ($proof in $Proofs) {
        if ((Get-FileHash -LiteralPath (Join-Path $RepositoryRoot $proof.file) -Algorithm SHA256).Hash.ToLowerInvariant() -cne $proof.sha256) {
            throw "Proof source changed during verification or diagnostics: $($proof.file)"
        }
    }
}

function Assert-ProofComplexityResult($Result, [string] $ResourceLog, [int] $Mutations) {
    if ($Result.ExitCode -ne 0 -or -not [IO.File]::Exists($ResourceLog)) { throw 'Complexity measurement failed or did not retain its resource log.' }
    $starts = [regex]::Matches($Result.Stdout, '(?m)^Starting verification of mutation (?<index>[0-9]+)/(?<count>[0-9]+) with seed (?<seed>[0-9]+)\r?$')
    if ($starts.Count -ne $Mutations) { throw 'Complexity measurement did not execute every requested mutation.' }
    $seeds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    for ($index = 0; $index -lt $starts.Count; $index++) {
        if ([int]$starts[$index].Groups['index'].Value -ne $index + 1 -or
            [int]$starts[$index].Groups['count'].Value -ne $Mutations -or
            -not $seeds.Add($starts[$index].Groups['seed'].Value)) { throw 'Complexity mutation inventory is invalid.' }
    }
    $rows = @(Import-Csv -LiteralPath $ResourceLog)
    if ($rows.Count -eq 0) { throw 'Complexity measurement selected no proof obligations.' }
    $bySeed = @{}
    $byObligation = [Collections.Generic.Dictionary[string, Collections.Generic.List[ulong]]]::new([StringComparer]::Ordinal)
    $maximum = [ulong]0
    foreach ($row in $rows) {
        $resources = [ulong]0
        $duration = [TimeSpan]::Zero
        if ([string]::IsNullOrWhiteSpace($row.'TestResult.DisplayName') -or $row.'TestResult.Outcome' -cne 'Passed' -or
            -not $seeds.Contains($row.RandomSeed) -or
            -not [ulong]::TryParse($row.'TestResult.ResourceCount', [Globalization.NumberStyles]::None, [Globalization.CultureInfo]::InvariantCulture, [ref]$resources) -or
            -not [TimeSpan]::TryParse($row.'TestResult.Duration', [Globalization.CultureInfo]::InvariantCulture, [ref]$duration) -or $duration -lt [TimeSpan]::Zero) {
            throw 'Complexity resource log contains failed or malformed proof evidence.'
        }
        if (-not $bySeed.ContainsKey($row.RandomSeed)) { $bySeed[$row.RandomSeed] = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal) }
        if (-not $bySeed[$row.RandomSeed].Add($row.'TestResult.DisplayName')) { throw 'Complexity resource log contains duplicate obligations.' }
        if (-not $byObligation.ContainsKey($row.'TestResult.DisplayName')) { $byObligation[$row.'TestResult.DisplayName'] = [Collections.Generic.List[ulong]]::new() }
        $byObligation[$row.'TestResult.DisplayName'].Add($resources)
        $maximum = [Math]::Max($maximum, $resources)
    }
    $baseline = $bySeed[$starts[0].Groups['seed'].Value]
    foreach ($seed in $seeds) {
        if (-not $bySeed.ContainsKey($seed) -or -not $bySeed[$seed].SetEquals($baseline)) { throw 'Complexity resource log has incomplete mutation coverage.' }
    }
    # Compare the same obligation across seeds, not unrelated assertions with
    # different costs. Population variation describes these measured seeds;
    # it neither predicts all seeds nor imposes a new acceptance threshold.
    $statistics = [Collections.Generic.List[object]]::new()
    foreach ($entry in $byObligation.GetEnumerator()) {
        $minimum = [ulong]::MaxValue
        $maximumForObligation = [ulong]0
        $sum = [double]0
        foreach ($sample in $entry.Value) {
            $minimum = [Math]::Min($minimum, $sample)
            $maximumForObligation = [Math]::Max($maximumForObligation, $sample)
            $sum += [double]$sample
        }
        $mean = $sum / $entry.Value.Count
        $squaredDifferences = [double]0
        foreach ($sample in $entry.Value) { $squaredDifferences += [Math]::Pow([double]$sample - $mean, 2) }
        $deviation = [Math]::Sqrt($squaredDifferences / $entry.Value.Count)
        $statistics.Add([pscustomobject][ordered]@{
            obligation = $entry.Key; samples = $entry.Value.Count
            minimumResources = $minimum; maximumResources = $maximumForObligation
            meanResources = $mean; populationStandardDeviation = $deviation
            resourceCoefficientOfVariationPercent = $(if ($mean -eq 0) { [double]0 } else { 100 * $deviation / $mean })
        })
    }
    $statistics.Sort([Comparison[object]]{
        param($left, $right)
        $comparison = $right.resourceCoefficientOfVariationPercent.CompareTo($left.resourceCoefficientOfVariationPercent)
        if ($comparison -ne 0) { return $comparison }
        return [StringComparer]::Ordinal.Compare($left.obligation, $right.obligation)
    })
    $highestVariation = @($statistics | Select-Object -First 10)
    $statistics.Sort([Comparison[object]]{
        param($left, $right)
        $comparison = $right.maximumResources.CompareTo($left.maximumResources)
        if ($comparison -ne 0) { return $comparison }
        return [StringComparer]::Ordinal.Compare($left.obligation, $right.obligation)
    })
    return [ordered]@{
        seeds = @($starts | ForEach-Object { $_.Groups['seed'].Value }); obligationsPerSeed = $baseline.Count; maximumResources = $maximum
        highestResourceVariation = $highestVariation; highestResourceUse = @($statistics | Select-Object -First 10)
    }
}

function Invoke-ProofComplexity([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [string[]] $Files, [string[]] $Options, [int] $TimeoutSeconds, [object[]] $Proofs, [string] $DafnyVersion, [string] $SolverVersion, [hashtable] $Diagnostics) {
    $closure = @($Files | ForEach-Object { [IO.Path]::GetRelativePath($RepositoryRoot, $_).Replace('\', '/') })
    $resourceLog = Join-Path $OutputDirectory 'proof-complexity.resources.csv'
    $receipt = [ordered]@{
        schemaVersion = 1; status = 'failed'; acceptanceEvidence = $false
        dafny = $DafnyVersion; solver = $SolverVersion
        requestedSources = @($Diagnostics.complexitySources); dependencyClosure = $closure
        position = $Diagnostics.position; mutations = $Diagnostics.mutations
        proofs = @($Proofs | Where-Object { $_.file -cin $closure } | ForEach-Object { [ordered]@{ file = $_.file; sha256 = $_.sha256 } })
        resourceLog = 'proof-complexity.resources.csv'; process = 'proof-complexity.process.json'
        stdoutLog = 'proof-complexity.stdout.log'; stderrLog = 'proof-complexity.stderr.log'
        statistics = $null; failure = $null
    }
    try {
        if ($receipt.proofs.Count -ne $Files.Count) { throw 'Complexity source hashes do not match the selected dependency closure.' }
        $arguments = @('measure-complexity') + $Files + $Options + @('--mutations', $Diagnostics.mutations.ToString([Globalization.CultureInfo]::InvariantCulture),
            '--isolate-assertions', '--log-format', "csv;LogFileName=$resourceLog")
        $position = Get-ProofDiagnosticPosition $RepositoryRoot $Diagnostics.complexitySources $Diagnostics.position
        if ($position) { $arguments += @('--filter-position', $position) }
        $result = Invoke-ProofProcess $DafnyPath $arguments $OutputDirectory 'proof-complexity' $TimeoutSeconds
        $receipt.statistics = Assert-ProofComplexityResult $result $resourceLog $Diagnostics.mutations
        Assert-ProofInputsUnchanged $RepositoryRoot $Proofs
        $receipt.status = 'complete'
        Write-Output "Supplemental proof complexity: $($receipt.statistics.obligationsPerSeed) obligations across $($Diagnostics.mutations) seeds. Results do not establish specification adequacy."
        $variable = $receipt.statistics.highestResourceVariation[0]
        $variation = $variable.resourceCoefficientOfVariationPercent.ToString('0.##', [Globalization.CultureInfo]::InvariantCulture)
        Write-Output "Highest measured resource variation: $($variable.obligation), population CV $variation%. Resource rankings are diagnostic, not an acceptance threshold."
    } catch { $receipt.failure = $_.Exception.Message; throw }
    finally { [IO.File]::WriteAllText((Join-Path $OutputDirectory 'proof-complexity.json'), ($receipt | ConvertTo-Json -Depth 6)) }
}

function Invoke-ProofCoverage([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [string[]] $Files, [string[]] $Options, [int] $TimeoutSeconds, [object[]] $Proofs, [string] $DafnyVersion, [string] $SolverVersion, [string[]] $RequestedSources = $null, [hashtable] $Diagnostics = @{}) {
    $dependencyLog = Join-Path $OutputDirectory 'proof-coverage.dependencies.log'
    $coverageDirectory = Join-Path $OutputDirectory 'proof-coverage'
    $requested = @()
    if ($null -ne $RequestedSources) { $requested = @($RequestedSources) }
    $closure = @($Files | ForEach-Object { [IO.Path]::GetRelativePath($RepositoryRoot, $_).Replace('\', '/') })
    $receipt = [ordered]@{
        schemaVersion = 1
        status = 'failed'
        acceptanceEvidence = $false
        scope = $(if ($null -eq $RequestedSources) { 'complete-inventory' } else { 'selected' })
        requestedSources = $requested
        dependencyClosure = $closure
        dafny = $DafnyVersion
        solver = $SolverVersion
        proofs = @($Proofs | Where-Object { $_.file -cin $closure } | ForEach-Object { [ordered]@{ file = $_.file; sha256 = $_.sha256 } })
        dependencyLog = 'proof-coverage.dependencies.log'
        coverageReport = 'proof-coverage/index_verification.html'
        stdoutLog = 'proof-coverage.verify.stdout.log'
        stderrLog = 'proof-coverage.verify.stderr.log'
        process = 'proof-coverage.verify.process.json'
        exitCode = $null
        warnings = @()
        failure = $null
        suggestProofRefactoring = [bool]$Diagnostics.suggestRefactoring
        position = $Diagnostics.position
        negativeVacuity = [ordered]@{
            status = 'not-run'; source = 'VacuousTheorem.dfy'
            process = 'proof-coverage.negative-vacuity.process.json'
            stdoutLog = 'proof-coverage.negative-vacuity.stdout.log'; stderrLog = 'proof-coverage.negative-vacuity.stderr.log'
        }
    }
    try {
        if ($receipt.proofs.Count -ne $Files.Count) { throw 'Coverage source hashes do not match the selected dependency closure.' }
        # This intentionally unreachable theorem stays outside the admitted
        # inventory. A nonconstant false postcondition exercises the warning;
        # literal `ensures false` is a legitimate contradiction lemma in Dafny.
        $receipt.negativeVacuity.status = 'failed'
        $vacuousPath = Join-Path $OutputDirectory $receipt.negativeVacuity.source
        [IO.File]::WriteAllText($vacuousPath, 'lemma VacuousTheorem(value: int) requires false ensures value == value + 1 {}')
        $vacuity = Invoke-ProofProcess $DafnyPath (@('verify', $vacuousPath) + $Options + @('--isolate-assertions', '--warn-contradictory-assumptions')) $OutputDirectory 'proof-coverage.negative-vacuity' $TimeoutSeconds
        $vacuityMessages = $vacuity.Stdout + "`n" + $vacuity.Stderr
        $vacuityComplete = [regex]::Matches($vacuity.Stdout, '(?m)^Dafny program verifier finished with 1 verified, 0 errors\r?$').Count -eq 1
        $vacuityWarning = $vacuityMessages -match '(?m)^[^\r\n]*VacuousTheorem\.dfy\(1,[0-9]+\):\s*Warning:\s*ensures clause proved using contradictory assumptions\r?$'
        $vacuityErrors = $vacuityMessages -match '(?m)^(?:[^\r\n]*\(\d+,\d+\):\s*)?Error:'
        if ($vacuity.ExitCode -ne 2 -or -not $vacuityComplete -or -not $vacuityWarning -or $vacuityErrors) {
            throw 'Contradictory-assumption negative control did not produce the expected warning and warning exit; inspect the retained control logs.'
        }
        $receipt.negativeVacuity.status = 'complete'
        # Dependency analysis is supplemental. It never relaxes the preceding
        # verification, audit, negative controls or generated-source checks.
        $analysisOptions = @('--isolate-assertions', '--warn-contradictory-assumptions', '--warn-redundant-assumptions',
            '--log-format', "text;LogFileName=$dependencyLog", '--verification-coverage-report', $coverageDirectory,
            '--no-timestamp-for-coverage-report')
        if ($Diagnostics.suggestRefactoring) { $analysisOptions += @('--suggest-proof-refactoring', '--show-hints') }
        $position = Get-ProofDiagnosticPosition $RepositoryRoot $RequestedSources $Diagnostics.position
        if ($position) { $analysisOptions += @('--filter-position', $position) }
        $result = Invoke-ProofProcess $DafnyPath (@('verify') + $Files + $Options + $analysisOptions) $OutputDirectory 'proof-coverage.verify' $TimeoutSeconds
        $receipt.exitCode = $result.ExitCode
        $messages = $result.Stdout + "`n" + $result.Stderr
        $receipt.warnings = @([regex]::Matches($messages, '(?m)^(?:[^\r\n]*\(\d+,\d+\):\s*)?Warning:\s*[^\r\n]*') | ForEach-Object Value)
        $complete = [regex]::Matches($result.Stdout, '(?m)^Dafny program verifier finished with [1-9][0-9]* (?:assertions )?verified, 0 errors\r?$').Count -eq 1
        $errors = $messages -match '(?m)^(?:[^\r\n]*\(\d+,\d+\):\s*)?Error:'
        $warningExit = $result.ExitCode -eq 2 -and $receipt.warnings.Count -gt 0
        if (-not $complete -or $errors -or ($result.ExitCode -ne 0 -and -not $warningExit)) {
            throw "Proof coverage analysis failed (exit $($result.ExitCode)); inspect the retained process and output logs."
        }
        if (-not [IO.File]::Exists($dependencyLog) -or (Get-Item -LiteralPath $dependencyLog).Length -eq 0 -or
            -not [IO.File]::Exists((Join-Path $coverageDirectory 'index_verification.html'))) {
            throw 'Proof coverage analysis did not retain its dependency log and HTML report.'
        }
        # The final mandatory receipt covers every admitted source, including
        # files outside a selected diagnostic scope. None may drift afterward.
        Assert-ProofInputsUnchanged $RepositoryRoot $Proofs
        $receipt.status = if ($receipt.warnings.Count -gt 0) { 'warnings' } else { 'complete' }
        Write-Output "Supplemental proof coverage: $($receipt.status), $($receipt.warnings.Count) warnings. Reports diagnose dependencies; they do not establish specification adequacy."
    } catch {
        $receipt.failure = $_.Exception.Message
        throw
    } finally {
        [IO.File]::WriteAllText((Join-Path $OutputDirectory 'proof-coverage.json'), ($receipt | ConvertTo-Json -Depth 6))
    }
}

function Invoke-ProofVerificationCore([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [int] $TimeoutSeconds, [bool] $Update, [bool] $AnalyzeCoverage = $false, [string[]] $CoverageSources = $null, [hashtable] $Diagnostics = @{}) {
    if ($null -ne $CoverageSources -and -not $AnalyzeCoverage) { throw 'ProofCoverageSources requires AnalyzeProofCoverage.' }
    $output = $OutputDirectory
    if (-not [IO.Path]::IsPathFullyQualified($DafnyPath)) { throw 'DafnyPath must be absolute.' }
    $executable = (Resolve-Path -LiteralPath $DafnyPath).Path
    $toolchain = [IO.File]::ReadAllText((Join-Path $RepositoryRoot 'verification/config/dafny-toolchain.json')) | ConvertFrom-Json
    if ($toolchain.schemaVersion -ne 1) { throw 'Unsupported Dafny toolchain manifest schema.' }
    $version = Invoke-ProofProcess $executable @('--version') $output 'version' 10
    if ($version.ExitCode -ne 0 -or $version.Stdout.Trim() -ne $toolchain.versionOutput) { throw "Dafny version does not match the pinned toolchain: $($version.Stdout)" }
    $solver = Join-Path (Split-Path $executable -Parent) $(if ($IsWindows) { $toolchain.solver.windows } else { $toolchain.solver.linux })
    $solverVersion = Invoke-ProofProcess $solver @('--version') $output 'solver-version' 10
    if ($solverVersion.ExitCode -ne 0 -or $solverVersion.Stdout.Trim() -ne $toolchain.solver.versionOutput) { throw "Z3 version does not match the pinned toolchain: $($solverVersion.Stdout)" }
    $files = @(Get-ProofFiles $RepositoryRoot (Join-Path $RepositoryRoot 'verification/config/proofs.json'))
    $dependenciesByFile = Get-ProofDependencies $RepositoryRoot (Join-Path $RepositoryRoot 'verification/config/proofs.json') $files
    $coverageFiles = @()
    if ($AnalyzeCoverage) { $coverageFiles = @(Get-ProofCoverageFiles $RepositoryRoot $files $dependenciesByFile $CoverageSources) }
    $complexityFiles = @()
    if ($Diagnostics.measureComplexity) { $complexityFiles = @(Get-ProofCoverageFiles $RepositoryRoot $files $dependenciesByFile $Diagnostics.complexitySources) }
    if ($Diagnostics.position) {
        if ($AnalyzeCoverage) { $null = Get-ProofDiagnosticPosition $RepositoryRoot $CoverageSources $Diagnostics.position }
        if ($Diagnostics.measureComplexity) { $null = Get-ProofDiagnosticPosition $RepositoryRoot $Diagnostics.complexitySources $Diagnostics.position }
    }
    foreach ($file in $files) { Assert-ProofSource $file ([IO.File]::ReadAllText($file)) }
    $cores = [Math]::Max(1, [Math]::Min(4, [Environment]::ProcessorCount))
    $options = @('--cores', $cores.ToString([Globalization.CultureInfo]::InvariantCulture), '--verification-time-limit', '60', '--solver-path', $solver, '--verify-included-files')
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
    $summary = @(Invoke-ProofInventory $RepositoryRoot $executable $output $files $options $TimeoutSeconds)
    Write-Output "Verified and audited complete inventory: $($files.Count) sources with $cores verifier cores"
    $kernels = foreach ($kernelName in Get-NativeKernelNames) {
        $kernelSource = Join-Path $RepositoryRoot (Get-NativeKernelSource $kernelName)
        if ($kernelSource -cnotin $files) { throw "Native kernel is missing from the proof inventory: $kernelName" }
        Sync-NativeKernel $kernelName $RepositoryRoot $executable $output $dependenciesByFile[$kernelSource] $options $TimeoutSeconds $Update
    }
    if ($AnalyzeCoverage) { Invoke-ProofCoverage $RepositoryRoot $executable $output $coverageFiles $options $TimeoutSeconds $summary ($version.Stdout.Trim()) ($solverVersion.Stdout.Trim()) $CoverageSources $Diagnostics }
    if ($Diagnostics.measureComplexity) { Invoke-ProofComplexity $RepositoryRoot $executable $output $complexityFiles $options $TimeoutSeconds $summary ($version.Stdout.Trim()) ($solverVersion.Stdout.Trim()) $Diagnostics }
    $receipt = [ordered]@{ schemaVersion = 2; dafny = $version.Stdout.Trim(); solver = $solverVersion.Stdout.Trim(); negativeControls = 'false theorem rejected; assumption audit detected'; proofs = $summary; executableKernels = @($kernels) }
    [IO.File]::WriteAllText((Join-Path $output 'summary.json'), ($receipt | ConvertTo-Json -Depth 6))
    Write-Output "Proofs and generated native kernels verified; handwritten code and storage assumptions remain separate obligations. Results: $output"
}

function Test-ProofDiagnosticLocalInput([string] $RepositoryRoot, [string] $Path) {
    $root = [IO.Path]::GetFullPath($RepositoryRoot).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    $full = [IO.Path]::GetFullPath($Path)
    $comparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
    if (-not $full.Equals($root, $comparison) -and -not $full.StartsWith($root + [IO.Path]::DirectorySeparatorChar, $comparison)) { throw 'Diagnostic input escapes the repository.' }
    if ([IO.File]::GetAttributes($root) -band [IO.FileAttributes]::ReparsePoint) { return $false }
    $current = $root
    $relative = [IO.Path]::GetRelativePath($root, $full).Replace('\', '/')
    foreach ($part in $relative.Split('/')) {
        $current = Join-Path $current $part
        if ([IO.File]::GetAttributes($current) -band [IO.FileAttributes]::ReparsePoint) { return $false }
    }
    return $true
}

function Write-ProofFailureReceipt([string] $RepositoryRoot, [string] $OutputDirectory, [string] $Failure) {
    # Snapshot failures are supplemental evidence; they never authorize following
    # linked inputs or replacing the original verifier error.
    $paths = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $snapshotErrors = [Collections.Generic.List[string]]::new()
    try {
        foreach ($file in Get-ChildItem -LiteralPath $RepositoryRoot -Filter '*.dfy' -File -Recurse -Force) { [void]$paths.Add($file.FullName) }
        $verification = Join-Path $RepositoryRoot 'verification'
        if (Test-ProofDiagnosticLocalInput $RepositoryRoot $verification) {
            foreach ($file in Get-ChildItem -LiteralPath $verification -Filter '*.json' -File -Recurse -Force) { [void]$paths.Add($file.FullName) }
        } else { $snapshotErrors.Add('Refused linked verification directory.') }
    } catch { $snapshotErrors.Add($_.Exception.Message) }
    $fixedInputs = @('scripts/verify-proofs.ps1', 'scripts/install-dafny.ps1', 'scripts/check-proof-review.ps1') + @(Get-NativeKernelNames | ForEach-Object { "src/JetDatabaseWriter/Verification/$_.g.cs" })
    foreach ($relative in $fixedInputs) {
        $path = Join-Path $RepositoryRoot $relative
        if ([IO.File]::Exists($path)) { [void]$paths.Add($path) }
    }
    $inputs = foreach ($path in @($paths | Sort-Object -CaseSensitive)) {
        $relative = [IO.Path]::GetRelativePath($RepositoryRoot, $path).Replace('\', '/')
        try {
            if (-not (Test-ProofDiagnosticLocalInput $RepositoryRoot $path)) {
                $snapshotErrors.Add("Refused linked diagnostic input: $relative")
                [ordered]@{ path = $relative; status = 'refused-linked' }
                continue
            }
            $copy = Join-Path $OutputDirectory "inputs/$relative"
            [void][IO.Directory]::CreateDirectory((Split-Path $copy -Parent))
            [IO.File]::Copy($path, $copy)
            [ordered]@{ path = $relative; status = 'retained'; sha256 = (Get-FileHash -LiteralPath $copy -Algorithm SHA256).Hash.ToLowerInvariant() }
        } catch {
            $snapshotErrors.Add("${relative}: $($_.Exception.Message)")
            [ordered]@{ path = $relative; status = 'snapshot-failed'; error = $_.Exception.Message }
        }
    }
    $artifacts = @(Get-ChildItem -LiteralPath $OutputDirectory -File | Sort-Object Name | ForEach-Object Name)
    $receipt = [ordered]@{
        schemaVersion = 1
        status = 'failed'
        failure = $Failure
        githubSha = $env:GITHUB_SHA
        negativeControls = 'Expected false-theorem verification and assumed-theorem audit failures are controls, not admitted proof inputs. This receipt records a genuine gate failure.'
        inputs = @($inputs)
        snapshotErrors = @($snapshotErrors.ToArray())
        artifacts = $artifacts
    }
    [IO.File]::WriteAllText((Join-Path $OutputDirectory 'failure.json'), ($receipt | ConvertTo-Json -Depth 6))
}

function Invoke-ProofVerification([string] $RepositoryRoot, [string] $DafnyPath, [string] $OutputDirectory, [int] $TimeoutSeconds, [bool] $Update, [bool] $AnalyzeCoverage = $false, [string[]] $CoverageSources = $null, [hashtable] $Diagnostics = @{}) {
    if ($null -ne $CoverageSources -and -not $AnalyzeCoverage) { throw 'ProofCoverageSources requires AnalyzeProofCoverage.' }
    if ($Diagnostics.suggestRefactoring -and -not $AnalyzeCoverage) { throw 'SuggestProofRefactoring requires AnalyzeProofCoverage.' }
    if ($Diagnostics.measureComplexity) {
        if (@($Diagnostics.complexitySources).Count -eq 0 -or $null -eq $Diagnostics.complexitySources -or
            $Diagnostics.mutations -lt 2 -or $Diagnostics.mutations -gt 20) { throw 'MeasureProofComplexity requires explicit ProofComplexitySources and between 2 and 20 mutations.' }
    } elseif ($null -ne $Diagnostics.complexitySources) { throw 'ProofComplexitySources requires MeasureProofComplexity.' }
    if ($Diagnostics.position -and -not ($AnalyzeCoverage -or $Diagnostics.measureComplexity)) { throw 'ProofDiagnosticPosition requires an explicit diagnostic mode.' }
    $output = Assert-ProofOutputDirectory $OutputDirectory $RepositoryRoot
    if (Test-Path -LiteralPath $output) { throw 'Proof output directory must not already exist; retain each run separately.' }
    [void][IO.Directory]::CreateDirectory($output)
    try { Invoke-ProofVerificationCore $RepositoryRoot $DafnyPath $output $TimeoutSeconds $Update $AnalyzeCoverage $CoverageSources $Diagnostics }
    catch {
        $originalFailure = $_
        try { Write-ProofFailureReceipt $RepositoryRoot $output $originalFailure.Exception.Message }
        catch {
            $snapshotFailure = $_.Exception.Message
            try { [IO.File]::WriteAllText((Join-Path $output 'failure-receipt.error.txt'), $snapshotFailure) }
            catch { [Console]::Error.WriteLine("Could not retain proof failure receipt: $snapshotFailure") }
        }
        throw $originalFailure
    }
}

Invoke-ProofVerification (Split-Path $PSScriptRoot -Parent) $DafnyPath $OutputDirectory $TimeoutSeconds $UpdateGeneratedSource.IsPresent $AnalyzeProofCoverage.IsPresent $ProofCoverageSources @{
    suggestRefactoring = $SuggestProofRefactoring.IsPresent; measureComplexity = $MeasureProofComplexity.IsPresent
    complexitySources = $ProofComplexitySources; mutations = $ProofComplexityMutations; position = $ProofDiagnosticPosition
}
