# Exercises the hosted fuzz coordinator with in-memory process doubles, never fuzz binaries.
[CmdletBinding()]
param([string] $OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'JetDatabaseWriter-script-checks'))
$ErrorActionPreference = 'Stop'
$testRoot = Join-Path ([IO.Path]::GetFullPath($OutputDirectory)) ([Guid]::NewGuid().ToString('N'))
$fixtureRoot = Join-Path $testRoot 'fixture'
$fixtureScripts = Join-Path $fixtureRoot 'scripts'
$fixtureCorpus = Join-Path $fixtureRoot 'JetDatabaseWriter.Tests/Databases'
[IO.Directory]::CreateDirectory($fixtureScripts) | Out-Null
[IO.Directory]::CreateDirectory($fixtureCorpus) | Out-Null
$subject = Join-Path $fixtureScripts 'run-hosted-fuzz.ps1'
Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../run-hosted-fuzz.ps1') -Destination $subject
Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../fuzz-inputs.ps1') -Destination $fixtureScripts
[IO.File]::WriteAllBytes((Join-Path $fixtureCorpus 'sample.mdb'), [byte[]](0..255))
$runner = Join-Path $fixtureRoot 'mock-runner.exe'
[IO.File]::WriteAllText($runner, 'Not executable; Start-Process is mocked.')
$fuzzTestContext = [pscustomobject]@{ Processes = [Collections.Generic.List[object]]::new(); Mode = 'Pass' }
$fuzzChecks = [Collections.Generic.List[string]]::new()
function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { $fuzzChecks.Add($Message) }
}
function Start-Process {
    [CmdletBinding()]
    param($FilePath, $ArgumentList, $WorkingDirectory, $Environment, $WindowStyle,
        $RedirectStandardOutput, $RedirectStandardError, [switch] $PassThru)
    Assert-Check ($FilePath -eq $runner) 'The runner path changed.'
    Assert-Check ($Environment.DOTNET_GCHeapHardLimit -eq '0x20000000') 'The heap limit was not forwarded.'
    Assert-Check ($ArgumentList -contains 'none') 'Per-case parallelism was enabled.'
    Assert-Check ($WindowStyle -eq 'Hidden') 'The runner would open a visible window.'
    Assert-Check ([IO.Path]::IsPathFullyQualified($WorkingDirectory) -and $WorkingDirectory.StartsWith([IO.Path]::GetTempPath()) -and (Test-Path -LiteralPath $WorkingDirectory)) 'The runner working directory was not an absolute temporary directory.'
    Assert-Check ($Environment.Keys.Count -eq 2) 'The child environment redirected inherited system/profile paths.'
    [IO.File]::WriteAllText($RedirectStandardOutput, '[FuzzCoverage] reader:open')
    [IO.File]::WriteAllText($RedirectStandardError, '')
    if ($fuzzTestContext.Mode -eq 'LaunchFailed') { throw 'Synthetic launch failure' }
    $xmlPath = $ArgumentList[-1].Trim('"')
    if ($fuzzTestContext.Mode -eq 'CompleteCoverage') {
        $tags = @('open:completed', 'catalog:completed', 'schema:completed', 'index-list:completed', 'rows:completed', 'seek:completed', 'links:completed', 'relationships:completed', 'encryption-attempt', 'encrypted:completed', 'link-denied') | ForEach-Object { "[FuzzCoverage] reader:$_" }
        [IO.File]::WriteAllText($xmlPath, ('<assemblies><test result="Pass"><output><![CDATA[{0}]]></output></test></assemblies>' -f ($tags -join [Environment]::NewLine)))
    } elseif ($fuzzTestContext.Mode -eq 'Pass') {
        [IO.File]::WriteAllText($xmlPath, '<assemblies><assembly><collection><test result="Pass" /></collection></assembly></assemblies>')
    } elseif ($fuzzTestContext.Mode -eq 'XmlCoverage') {
        [IO.File]::WriteAllText($RedirectStandardOutput, '')
        [IO.File]::WriteAllText($xmlPath, '<assemblies><test result="Pass"><output><![CDATA[[FuzzCoverage] reader:seek:completed
[FuzzCoverage] writer.insert.succeeded
[FuzzCoverage] schema.write.refused]]></output></test></assemblies>')
    } elseif ($fuzzTestContext.Mode -in 'FailureA', 'FailureB') {
        [IO.File]::WriteAllText($xmlPath, ('<assemblies><test result="Fail"><failure exception-type="{0}"><message>input-dependent</message><stack-trace>at Fuzz.Harness in C:\agent\file.cs:line 17</stack-trace></failure></test></assemblies>' -f $fuzzTestContext.Mode))
    } elseif ($fuzzTestContext.Mode -eq 'Unexpected') {
        [IO.File]::WriteAllText($xmlPath, '<assemblies><test result="Skip" /></assemblies>')
    } elseif ($fuzzTestContext.Mode -eq 'Malformed') {
        [IO.File]::WriteAllText($xmlPath, '<assemblies>')
    }
    $process = [pscustomobject]@{
        ExitCode = $(if ($fuzzTestContext.Mode -in 'Failed', 'FailureA', 'FailureB') { 7 } else { 0 })
        HasExited = ($fuzzTestContext.Mode -notin 'MemoryLimit', 'Timeout')
        PrivateMemorySize64 = 513MB
        Mode = $fuzzTestContext.Mode
        KilledTree = $false
        Disposed = $false
    }
    $process | Add-Member ScriptMethod WaitForExit {
        param([int] $Milliseconds)
        if ($Milliseconds -eq 0) { return }
        if ($this.Mode -eq 'Timeout' -and -not $this.HasExited) { Start-Sleep -Milliseconds 1100 }
        return $this.HasExited
    }
    $process | Add-Member ScriptMethod Refresh { }
    $process | Add-Member ScriptMethod Kill {
        param([bool] $EntireProcessTree)
        $this.KilledTree = $EntireProcessTree
        $this.HasExited = $true
    }
    $process | Add-Member ScriptMethod Dispose { $this.Disposed = $true }
    $fuzzTestContext.Processes.Add($process)
    return $process
}
function Invoke-Scenario([string] $Name, [string] $Mode, [int] $Cases, [int] $ExpectedCases = 0) {
    if ($ExpectedCases -eq 0) { $ExpectedCases = 3 * $Cases }
    $fuzzTestContext.Mode = $Mode
    $fuzzTestContext.Processes.Clear()
    $output = Join-Path $testRoot $Name
    [IO.Directory]::CreateDirectory($output) | Out-Null
    if ($Mode -eq 'Missing') {
        foreach ($kind in @('Reader', 'Writer', 'Text')) {
            [IO.File]::WriteAllText((Join-Path $output "$kind-0000.xml"), '<assemblies><test result="Pass" /></assemblies>')
        }
    }
    $caught = $null
    try { & $subject -Runner $runner -Cases $Cases -Seed 42 -TimeoutSeconds 1 -MinimizeAttempts 1 -MinimizeSeconds 2 -OutputDirectory $output }
    catch { $caught = $_ }
    $manifestPath = Join-Path $output 'manifest.json'
    Assert-Check (Test-Path -LiteralPath $manifestPath) "$Name did not preserve its manifest. Error: $caught"
    Assert-Check ($fuzzTestContext.Processes.Count -ge $ExpectedCases -and $fuzzTestContext.Processes.Count -le (2 * $ExpectedCases) -or $Mode -eq 'LaunchFailed') "$Name did not run every case within its minimization bound."
    Assert-Check (@($fuzzTestContext.Processes | Where-Object { -not $_.Disposed }).Count -eq 0) "$Name leaked a process handle."
    if ($Mode -in 'MemoryLimit', 'Timeout') {
        Assert-Check (@($fuzzTestContext.Processes | Where-Object { -not $_.KilledTree }).Count -eq 0) "$Name did not terminate the process tree."
    }
    $manifest = if (Test-Path -LiteralPath $manifestPath) { @(Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json) } else { @() }
    Assert-Check (Test-Path -LiteralPath (Join-Path $output 'coverage-manifest.json')) "$Name lost its coverage manifest."
    foreach ($entry in $manifest) {
        Assert-Check (-not [string]::IsNullOrEmpty($entry.Method) -and $entry.InputBytes -ge 0 -and $null -ne $entry.Mutation) "$Name omitted reproducibility metadata."
        if ($entry.Status -ne 'Passed') {
            Assert-Check (Test-Path -LiteralPath (Join-Path $output $entry.Input)) "$Name lost its original reproduction."
            Assert-Check (-not [string]::IsNullOrEmpty($entry.Signature)) "$Name omitted the original failure signature."
            if ($entry.Minimized) {
                Assert-Check (Test-Path -LiteralPath (Join-Path $output $entry.Minimized.Input)) "$Name lost its minimized reproduction."
                Assert-Check ($entry.Minimized.Attempts -le 1 -and $entry.Minimized.Signature -eq $entry.Signature) "$Name violated its shrink budget or failure signature."
            }
        }
    }
    return [pscustomobject]@{ Output = $output; Manifest = $manifest; Error = $caught }
}
$pass = Invoke-Scenario 'pass' 'Pass' 4
Assert-Check ($null -eq $pass.Error) "Passing cases failed: $($pass.Error)"
Assert-Check (@($pass.Manifest | Where-Object Status -ne 'Passed').Count -eq 0) 'A passing case had the wrong status.'
Assert-Check (@(Get-ChildItem -LiteralPath $pass.Output -Filter '*.bin').Count -eq 0) 'Passing inputs were retained.'
Assert-Check (@($pass.Manifest | Where-Object { $_.OperationCoverage -contains 'reader:open' }).Count -eq 12) 'Operation reachability tags were not recorded.'
$repeat = Invoke-Scenario 'repeat' 'Pass' 4
Assert-Check (($pass.Manifest.SHA256 -join ',') -eq ($repeat.Manifest.SHA256 -join ',')) 'The seed did not reproduce the same input hashes.'
$xmlCoverage = Invoke-Scenario 'xml-coverage' 'XmlCoverage' 1
Assert-Check ($null -eq $xmlCoverage.Error -and @($xmlCoverage.Manifest | Where-Object { $_.OperationCoverage -contains 'reader:seek:completed' }).Count -eq 3) 'Passing xUnit XML output did not contribute operation coverage.'
Assert-Check (@($xmlCoverage.Manifest | Where-Object { $_.OperationCoverage -contains 'writer.insert.succeeded' -and $_.OperationCoverage -contains 'schema.write.refused' }).Count -eq 3) 'Dotted writer operation/outcome markers were truncated.'
$failureA = Invoke-Scenario 'failure-signature-a' 'FailureA' 1
$failureB = Invoke-Scenario 'failure-signature-b' 'FailureB' 1
Assert-Check ($failureA.Manifest[0].Signature -ne $failureB.Manifest[0].Signature) 'Different exception types produced the same shrink signature.'
Assert-Check ($failureA.Manifest[0].Signature -eq $failureA.Manifest[0].Minimized.Signature) 'Failure identity was not preserved in the minimized reproduction.'
$missing = Invoke-Scenario 'stale-results' 'Missing' 1
Assert-Check ($null -ne $missing.Error) 'Stale XML results falsely passed.'
Assert-Check (@($missing.Manifest | Where-Object Status -eq 'MissingResult').Count -eq 3) 'Missing current results were not recorded.'
Assert-Check (@(Get-ChildItem -LiteralPath $missing.Output -Filter '*.bin').Count -ge 3) 'Missing-result input evidence was deleted.'
$malformed = Invoke-Scenario 'malformed-results' 'Malformed' 1
Assert-Check ($null -ne $malformed.Error) 'Malformed XML results falsely passed.'
Assert-Check (@($malformed.Manifest | Where-Object Status -eq 'InvalidResult').Count -eq 3) 'Malformed results were not recorded.'
Assert-Check (@(Get-ChildItem -LiteralPath $malformed.Output -Filter '*.bin').Count -ge 3) 'Malformed-result input evidence was not retained.'
foreach ($mode in @('Failed', 'Unexpected', 'MemoryLimit', 'Timeout', 'LaunchFailed')) {
    $run = Invoke-Scenario $mode $mode 1
    $expectedStatus = if ($mode -eq 'Unexpected') { 'UnexpectedResult' } elseif ($mode -eq 'LaunchFailed') { 'LaunchError' } else { $mode }
    Assert-Check ($null -ne $run.Error) "$mode cases falsely passed."
    Assert-Check (@($run.Manifest | Where-Object Status -eq $expectedStatus).Count -eq 3) "$mode cases had the wrong status."
    Assert-Check (@(Get-ChildItem -LiteralPath $run.Output -Filter '*.bin').Count -ge 3) "$mode input evidence was deleted."
}
$cutoff = Invoke-Scenario 'failure-cutoff' 'Failed' 4 4
Assert-Check ($cutoff.Manifest.Count -eq 4 -and $null -ne $cutoff.Error) 'The campaign ignored its global primary failure limit.'
$cutoffCoverage = Get-Content -LiteralPath (Join-Path $cutoff.Output 'coverage-manifest.json') -Raw | ConvertFrom-Json
Assert-Check ($cutoffCoverage.Aborted) 'An early failure cutoff was not reported as incomplete.'

# Enable a real metadata schedule from synthetic pages, without invoking a binary.
. (Join-Path $PSScriptRoot '../fuzz-inputs.ps1')
$structured = [byte[]]::new(4096 * 5)
[Text.Encoding]::ASCII.GetBytes("Standard Jet DB`0").CopyTo($structured, 4)
$structured[0x14] = 1
Protect-FuzzHeader $structured
$structured[4096] = 1
$structured[4096 + 12] = 1
[BitConverter]::GetBytes([uint16](4096 - 69)).CopyTo($structured, 4096 + 14)
$structured[8192] = 2
$structured[8192 + 56] = 1
$structured[12288] = 4
$structured[16384] = 5
[IO.File]::WriteAllBytes((Join-Path $fixtureCorpus 'structure.mdb'), $structured)
$incomplete = Invoke-Scenario 'incomplete-operation-coverage' 'Pass' 16
Assert-Check ($null -ne $incomplete.Error) 'A full campaign without completed operation markers falsely passed.'
$complete = Invoke-Scenario 'complete-operation-coverage' 'CompleteCoverage' 16
Assert-Check ($null -eq $complete.Error) "A fully covered synthetic campaign failed: $($complete.Error)"
$coverage = Get-Content -LiteralPath (Join-Path $complete.Output 'coverage-manifest.json') -Raw | ConvertFrom-Json
Assert-Check ($coverage.MissingGroups.Count -eq 0 -and $coverage.MissingOperations.Count -eq 0) 'The full synthetic campaign omitted mutation groups or completed operations.'
Assert-Check (@($complete.Manifest | Where-Object { $_.Mutation.Family -notin 'Control', 'Random', 'Truncate' -and $_.SourceSHA256.Length -eq 64 }).Count -ge 9) 'Structural campaigns omitted corpus or field provenance.'
Write-Host "Mocked fuzz artifacts: $testRoot"
if ($fuzzChecks.Count -gt 0) { throw ($fuzzChecks -join [Environment]::NewLine) }
Write-Host 'All mocked hosted fuzz checks passed.'
