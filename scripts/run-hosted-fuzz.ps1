# Runs explicit fuzz cases in separate, bounded processes using CI-built binaries.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $Runner,
    [ValidateRange(1, 512)][int] $Cases = 64,
    [ValidateRange(1, 120)][int] $TimeoutSeconds = 30,
    [ValidateRange(64, 2048)][int] $MemoryLimitMiB = 512,
    [ValidateRange(0, 32)][int] $MinimizeAttempts = 8,
    [ValidateRange(1, 60)][int] $MinimizeSeconds = 30,
    [ValidateRange(1, 64)][int] $FailureLimit = 4,
    [int] $Seed = 20261006,
    [string] $OutputDirectory = 'artifacts/fuzz'
)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'fuzz-inputs.ps1')
$repoRoot = Split-Path $PSScriptRoot -Parent
# workflow_run's GITHUB_SHA follows the default branch rather than the completed CI checkout.
$revisionSha = $env:JDW_VALIDATION_SHA
if ([string]::IsNullOrWhiteSpace($revisionSha)) {
    if ($env:GITHUB_EVENT_NAME -ne 'workflow_dispatch') { throw 'Automatic fuzzing requires the validated revision from the completed CI run.' }
    $revisionSha = $env:GITHUB_SHA
}
if ($revisionSha -cnotmatch '^[0-9a-f]{40}$') { throw 'The fuzz evidence revision must be a full commit ID.' }
$checkoutRevision = @(& git -C $repoRoot rev-parse HEAD)
if ($LASTEXITCODE -ne 0 -or $checkoutRevision.Count -ne 1 -or $checkoutRevision[0] -cne $revisionSha) { throw 'Fuzz evidence must match the checked-out revision.' }
$outputPath = [IO.Path]::GetFullPath($OutputDirectory)
if ($outputPath -match '%[^%]+%') { throw 'The output path contains an unresolved environment variable.' }
[IO.Directory]::CreateDirectory($outputPath) | Out-Null
$sessionRoot = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-fuzz-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($sessionRoot) | Out-Null
$manifest = [Collections.Generic.List[object]]::new()
$failures = 0
$campaignError = $null
$plan = $null
$requiredOperations = @('reader:open:completed', 'reader:catalog:completed', 'reader:schema:completed', 'reader:index-list:completed', 'reader:rows:completed', 'reader:seek:completed', 'reader:links:completed', 'reader:relationships:completed', 'reader:encryption-attempt', 'reader:encrypted:completed', 'reader:link-denied')
$methods = @{
    Reader = 'JetDatabaseWriter.Tests.Fuzz.AccessReaderFuzzTests.FuzzAccessReader'
    Writer = 'JetDatabaseWriter.Tests.Fuzz.AccessWriterFuzzTests.FuzzAccessWriter'
    Text = 'JetDatabaseWriter.Tests.Fuzz.DelimitedTextReaderFuzzTests.FuzzDelimitedTextReader'
}
function Get-Hash([byte[]] $Bytes) { return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($Bytes)) }
function Save-Manifests {
    [IO.File]::WriteAllText((Join-Path $outputPath 'manifest.json'), (ConvertTo-Json -InputObject @($manifest.ToArray()) -Depth 10))
    $executedGroups = @($manifest | Where-Object { $_.Kind -eq 'Reader' -and $_.Mutation.Family -notin 'Control', 'Random', 'Truncate' } | ForEach-Object { "$($_.Mutation.Format)/$($_.Mutation.Family)" } | Sort-Object -Unique)
    $availableGroups = if ($null -ne $plan) { @($plan.AvailableGroups) } else { @() }
    $coverage = [ordered]@{
        SchemaVersion = 2; Seed = $Seed; Revision = $revisionSha; Runner = $Runner
        CasesPerHarness = $Cases; TimeoutSeconds = $TimeoutSeconds; MemoryLimitMiB = $MemoryLimitMiB
        MinimizeAttempts = $MinimizeAttempts; MinimizeSeconds = $MinimizeSeconds; FailureLimit = $FailureLimit
        AvailableGroups = $availableGroups; ExecutedGroups = $executedGroups
        MissingGroups = @($availableGroups | Where-Object { $_ -notin $executedGroups })
        PlannedReaderCases = if ($null -ne $plan) { @($plan.Entries | Select-Object Family, Format, @{ Name = 'Source'; Expression = { [IO.Path]::GetRelativePath($repoRoot, $_.Source.File.FullName) } }) } else { @() }
        OperationCoverage = @($manifest.OperationCoverage | Sort-Object -Unique)
        OperationCounts = @($manifest.OperationCoverage | Group-Object | Sort-Object Name | Select-Object Name, Count)
        RequiredOperations = $requiredOperations
        MissingOperations = @($requiredOperations | Where-Object { $_ -notin $manifest.OperationCoverage })
        ExecutedCases = $manifest.Count; Failures = $failures
        Aborted = $null -ne $campaignError -or $failures -ge $FailureLimit; Error = $campaignError
    }
    [IO.File]::WriteAllText((Join-Path $outputPath 'coverage-manifest.json'), ($coverage | ConvertTo-Json -Depth 10))
}
function Invoke-FuzzProcess([string] $InputPath, [string] $Method, [string] $Prefix, [double] $DeadlineSeconds) {
    $xmlPath = "$Prefix.xml"
    $stdoutPath = "$Prefix.stdout.log"
    $stderrPath = "$Prefix.stderr.log"
    $workPath = Join-Path $sessionRoot ([Guid]::NewGuid().ToString('N'))
    [IO.Directory]::CreateDirectory($workPath) | Out-Null
    [IO.File]::Delete($xmlPath)
    [IO.File]::WriteAllText($stdoutPath, '')
    [IO.File]::WriteAllText($stderrPath, '')
    $arguments = @('-method', $Method, '-explicit', 'only', '-parallelMode', 'none', '-noColor', '-result-xml', ('"{0}"' -f $xmlPath))
    $environment = @{ JETDATABASEWRITER_FUZZ_INPUT = $InputPath; DOTNET_GCHeapHardLimit = ('0x{0:X}' -f ($MemoryLimitMiB * 1MB)) }
    $process = $null
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $status = 'Passed'
    $signature = $null
    $exitCode = $null
    $result = $null
    try {
        try {
            # Only task variables are overridden; system/profile paths remain inherited.
            $process = Start-Process -FilePath $runnerPath -ArgumentList $arguments -WorkingDirectory $workPath -Environment $environment -WindowStyle Hidden -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
        } catch {
            $status = 'LaunchError'
            $signature = 'LaunchError:' + $_.Exception.GetType().FullName
            [IO.File]::AppendAllText($stderrPath, $_.Exception.ToString())
        }
        if ($null -ne $process) {
            while (-not $process.WaitForExit(100)) {
                $process.Refresh()
                if ($timer.Elapsed.TotalSeconds -ge $DeadlineSeconds) { $status = 'Timeout'; break }
                if ($process.PrivateMemorySize64 -gt ($MemoryLimitMiB * 1MB)) { $status = 'MemoryLimit'; break }
            }
            if ($status -ne 'Passed') {
                if (-not $process.HasExited) { $process.Kill($true) }
                $process.WaitForExit()
            } else {
                $exitCode = $process.ExitCode
                if ($exitCode -ne 0) { $status = 'Failed' }
                if (Test-Path -LiteralPath $xmlPath) {
                    try { [xml]$result = [IO.File]::ReadAllText($xmlPath) }
                    catch {
                        if ($status -eq 'Passed') { $status = 'InvalidResult' }
                        [IO.File]::AppendAllText($stderrPath, ([Environment]::NewLine + $_.Exception.Message))
                    }
                } elseif ($status -eq 'Passed') { $status = 'MissingResult' }
                if ($null -ne $result) {
                    $tests = $result.SelectNodes('//test')
                    if ($status -eq 'Passed' -and ($tests.Count -ne 1 -or $tests[0].result -ne 'Pass')) { $status = 'UnexpectedResult' }
                    $failure = $result.SelectSingleNode('//test/failure')
                    if ($null -ne $failure) {
                        # Keep exception type and stable stack frames; paths and input-dependent messages vary while shrinking.
                        $stack = [regex]::Replace([string]$failure.'stack-trace', ' in [^\r\n]+:line (\d+)', ':line $1')
                        $frames = @(($stack -split '\r?\n') | Where-Object { $_.Trim().Length -gt 0 } | Select-Object -First 4)
                        $identity = [string]$failure.'exception-type' + '|' + ($frames -join '|')
                        $signature = $status + ':' + $exitCode + ':' + (Get-Hash ([Text.Encoding]::UTF8.GetBytes($identity)))
                    } elseif ($status -eq 'UnexpectedResult') {
                        $signature = 'UnexpectedResult:' + (($tests | ForEach-Object { $_.result }) -join ',')
                    }
                }
            }
        }
    } catch {
        $status = 'ProcessError'
        $signature = 'ProcessError:' + $_.Exception.GetType().FullName
        [IO.File]::AppendAllText($stderrPath, ([Environment]::NewLine + $_.Exception.ToString()))
    } finally {
        if ($null -ne $process) {
            try { if (-not $process.HasExited) { $process.Kill($true); $process.WaitForExit() } }
            catch {
                $status = 'ProcessError'
                $signature = 'ProcessError:' + $_.Exception.GetType().FullName
                [IO.File]::AppendAllText($stderrPath, ([Environment]::NewLine + $_.Exception.ToString()))
            }
            finally {
                try { $process.Dispose() }
                catch {
                    $status = 'ProcessError'
                    $signature = 'ProcessError:' + $_.Exception.GetType().FullName
                    [IO.File]::AppendAllText($stderrPath, ([Environment]::NewLine + $_.Exception.ToString()))
                }
            }
        }
        $timer.Stop()
    }
    if (-not $signature) { $signature = if ($status -eq 'Failed') { "Failed:$exitCode" } else { $status } }
    $coverageText = [IO.File]::ReadAllText($stdoutPath)
    if ($null -ne $result) { $coverageText += [Environment]::NewLine + $result.DocumentElement.InnerText }
    $operations = @([regex]::Matches($coverageText, '\[FuzzCoverage\]\s+([\w.:-]+)') | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
    return [pscustomobject]@{ Status = $status; Signature = $signature; ExitCode = $exitCode; Seconds = $timer.Elapsed.TotalSeconds; OperationCoverage = $operations }
}

try {
    $runnerPath = (Resolve-Path -LiteralPath $Runner).Path
    $corpus = @(Get-ChildItem -LiteralPath (Join-Path $repoRoot 'tests/JetDatabaseWriter.Tests/Databases') -Recurse -File |
        Where-Object { $_.Extension -in '.mdb', '.accdb' -and $_.Length -le 16MB } | Sort-Object FullName)
    if ($corpus.Count -eq 0) { throw 'No bounded database corpus found.' }
    $plan = Get-FuzzReaderPlan $corpus $Seed
    $random = [Random]::new($Seed)
    Save-Manifests
    foreach ($kind in @('Reader', 'Writer', 'Text')) {
        for ($case = 0; $case -lt $Cases -and $failures -lt $FailureLimit; $case++) {
            $caseName = '{0}-{1:D4}' -f $kind, $case
            $source = $null
            $sourceHash = $null
            $mutation = [pscustomobject]@{ Family = 'Random'; Format = $kind }
            if ($kind -eq 'Reader') {
                if ($case -lt $plan.Entries.Count) {
                    $entry = $plan.Entries[$case]
                    $fixture = $entry.Source
                    $bytes = [IO.File]::ReadAllBytes($fixture.File.FullName)
                    if ($null -eq $entry.Field) { $mutation = [pscustomobject]@{ Family = 'Control'; Format = $entry.Format } }
                    else {
                        $generated = New-FuzzStructuralInput $bytes $entry.Field $random.Next(4)
                        $bytes = $generated.Bytes
                        $mutation = $generated.Mutation
                    }
                } else {
                    $fixture = $plan.Catalog[($case + [Math]::Abs([long]$Seed)) % $plan.Catalog.Count]
                    $bytes = [IO.File]::ReadAllBytes($fixture.File.FullName)
                    if ($case % 3 -eq 0) {
                        $length = $random.Next($bytes.Length + 1)
                        $truncated = [byte[]]::new($length)
                        [Array]::Copy($bytes, $truncated, $length)
                        $bytes = $truncated
                        $mutation = [pscustomobject]@{ Family = 'Truncate'; Format = if ($fixture.Fields.Count) { $fixture.Fields[0].Format } else { 'Unknown' }; Length = $length }
                    } elseif ($case % 3 -eq 1 -and $fixture.Fields.Count -gt 0) {
                        $generated = New-FuzzStructuralInput $bytes $fixture.Fields[$random.Next($fixture.Fields.Count)] $random.Next(4)
                        $bytes = $generated.Bytes
                        $mutation = $generated.Mutation
                    } else {
                        $edits = @()
                        for ($edit = 0; $edit -lt 16 -and $bytes.Length -gt 0; $edit++) {
                            $position = $random.Next($bytes.Length)
                            $value = [byte]$random.Next(256)
                            $edits += [pscustomobject]@{ Offset = $position; Before = $bytes[$position]; After = $value }
                            $bytes[$position] = $value
                        }
                        $mutation = [pscustomobject]@{ Family = 'Random'; Format = if ($fixture.Fields.Count) { $fixture.Fields[0].Format } else { 'Unknown' }; Edits = $edits }
                    }
                }
                $source = [IO.Path]::GetRelativePath($repoRoot, $fixture.File.FullName)
                $sourceHash = $fixture.SHA256
            } else {
                $bytes = [byte[]]::new($random.Next(1, 4097))
                $random.NextBytes($bytes)
            }
            $inputPath = Join-Path $outputPath "$caseName.bin"
            [IO.File]::WriteAllBytes($inputPath, $bytes)
            $record = [pscustomobject]@{
                Case = $caseName; Kind = $kind; Seed = $Seed; MutationVersion = 2; Source = $source; SourceSHA256 = $sourceHash
                SHA256 = Get-Hash $bytes; Input = "$caseName.bin"; InputBytes = $bytes.Length; Method = $methods[$kind]; Mutation = $mutation
                Status = 'Pending'; Signature = 'Pending'; ExitCode = $null; Seconds = 0
                OperationCoverage = @(); Minimized = $null
            }
            $manifest.Add($record)
            Save-Manifests
            $outcome = Invoke-FuzzProcess $inputPath $methods[$kind] (Join-Path $outputPath $caseName) $TimeoutSeconds
            $record.Status = $outcome.Status
            $record.Signature = $outcome.Signature
            $record.ExitCode = $outcome.ExitCode
            $record.Seconds = $outcome.Seconds
            $record.OperationCoverage = $outcome.OperationCoverage
            Write-Host "$caseName $($outcome.Status) $($mutation.Family)"
            if ($outcome.Status -eq 'Passed') { [IO.File]::Delete($inputPath) }
            else {
                $failures++
                Save-Manifests
                if ($outcome.Status -notin 'LaunchError', 'ProcessError' -and $MinimizeAttempts -gt 0) {
                    # The probe runs in the minimizer scope, where $outcome changes after each attempt.
                    $failureSignature = $outcome.Signature
                    foreach ($suffix in @('.stdout.log', '.stderr.log', '.xml')) {
                        $originalLog = Join-Path $outputPath "$caseName$suffix"
                        if (Test-Path -LiteralPath $originalLog) { [IO.File]::Copy($originalLog, (Join-Path $outputPath "$caseName.minimized$suffix"), $true) }
                    }
                    $probe = {
                        param([byte[]] $Candidate, [double] $Remaining, [int] $Attempt)
                        $probePrefix = Join-Path $sessionRoot "$caseName-shrink-$Attempt"
                        $probeInput = "$probePrefix.bin"
                        [IO.File]::WriteAllBytes($probeInput, $Candidate)
                        $probeResult = Invoke-FuzzProcess $probeInput $methods[$kind] $probePrefix ([Math]::Min($TimeoutSeconds, $Remaining))
                        if ($probeResult.Signature -eq $failureSignature) {
                            foreach ($suffix in @('.stdout.log', '.stderr.log', '.xml')) {
                                if (Test-Path -LiteralPath "$probePrefix$suffix") { [IO.File]::Copy("$probePrefix$suffix", (Join-Path $outputPath "$caseName.minimized$suffix"), $true) }
                            }
                        }
                        return $probeResult
                    }
                    $minimumAttempt = if ($outcome.Status -eq 'Timeout') { $TimeoutSeconds } else { 0 }
                    $minimum = Invoke-FuzzMinimization -Bytes $bytes -Signature $outcome.Signature -Probe $probe -MaxAttempts $MinimizeAttempts -BudgetSeconds $MinimizeSeconds -MinimumAttemptSeconds $minimumAttempt
                    $minimumPath = "$caseName.minimized.bin"
                    [IO.File]::WriteAllBytes((Join-Path $outputPath $minimumPath), $minimum.Bytes)
                    $record.Minimized = [pscustomobject]@{ Input = $minimumPath; SHA256 = Get-Hash $minimum.Bytes; Signature = $minimum.Signature; Attempts = $minimum.Attempts; Seconds = $minimum.Seconds; OriginalBytes = $minimum.OriginalBytes; MinimizedBytes = $minimum.MinimizedBytes }
                }
            }
            Save-Manifests
        }
    }
    if ($failures -eq 0 -and $plan.AvailableGroups.Count -gt 0 -and $Cases -ge $plan.Entries.Count) {
        $missing = @($requiredOperations | Where-Object { $_ -notin $manifest.OperationCoverage })
        if ($missing.Count -gt 0) { throw "Reader operation reachability is incomplete: $($missing -join ', '). See coverage-manifest.json." }
    }
} catch {
    $campaignError = $_.Exception.ToString()
    throw
} finally { Save-Manifests }
if ($failures -gt 0) { throw "$failures fuzz cases failed; retained inputs, minimized reproductions and logs are in $outputPath." }
