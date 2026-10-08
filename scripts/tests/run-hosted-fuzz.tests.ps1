# Exercises the hosted fuzz coordinator with in-memory process doubles, never fuzz binaries.
[CmdletBinding()]
param([string] $OutputDirectory = 'artifacts/script-checks/fuzz')
$ErrorActionPreference = 'Stop'
$testRoot = Join-Path ([IO.Path]::GetFullPath($OutputDirectory)) ([Guid]::NewGuid().ToString('N'))
$fixtureRoot = Join-Path $testRoot 'fixture'
$fixtureScripts = Join-Path $fixtureRoot 'scripts'
$fixtureCorpus = Join-Path $fixtureRoot 'JetDatabaseWriter.Tests/Databases'
[IO.Directory]::CreateDirectory($fixtureScripts) | Out-Null
[IO.Directory]::CreateDirectory($fixtureCorpus) | Out-Null
$subject = Join-Path $fixtureScripts 'run-hosted-fuzz.ps1'
Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../run-hosted-fuzz.ps1') -Destination $subject
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
    [IO.File]::WriteAllText($RedirectStandardOutput, 'Mock runner output')
    [IO.File]::WriteAllText($RedirectStandardError, '')
    $xmlPath = $ArgumentList[-1].Trim('"')
    if ($fuzzTestContext.Mode -eq 'Pass') {
        [IO.File]::WriteAllText($xmlPath, '<assemblies><assembly><collection><test result="Pass" /></collection></assembly></assemblies>')
    } elseif ($fuzzTestContext.Mode -eq 'Unexpected') {
        [IO.File]::WriteAllText($xmlPath, '<assemblies><test result="Skip" /></assemblies>')
    } elseif ($fuzzTestContext.Mode -eq 'Malformed') {
        [IO.File]::WriteAllText($xmlPath, '<assemblies>')
    }
    $process = [pscustomobject]@{
        ExitCode = $(if ($fuzzTestContext.Mode -eq 'Failed') { 7 } else { 0 })
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
function Invoke-Scenario([string] $Name, [string] $Mode, [int] $Cases) {
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
    try { & $subject -Runner $runner -Cases $Cases -Seed 42 -TimeoutSeconds 1 -OutputDirectory $output }
    catch { $caught = $_ }
    $manifestPath = Join-Path $output 'manifest.json'
    Assert-Check (Test-Path -LiteralPath $manifestPath) "$Name did not preserve its manifest. Error: $caught"
    Assert-Check ($fuzzTestContext.Processes.Count -eq (3 * $Cases)) "$Name did not run every case."
    Assert-Check (@($fuzzTestContext.Processes | Where-Object { -not $_.Disposed }).Count -eq 0) "$Name leaked a process handle."
    if ($Mode -in 'MemoryLimit', 'Timeout') {
        Assert-Check (@($fuzzTestContext.Processes | Where-Object { -not $_.KilledTree }).Count -eq 0) "$Name did not terminate the process tree."
    }
    $manifest = if (Test-Path -LiteralPath $manifestPath) { @(Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json) } else { @() }
    return [pscustomobject]@{ Output = $output; Manifest = $manifest; Error = $caught }
}
$pass = Invoke-Scenario 'pass' 'Pass' 4
Assert-Check ($null -eq $pass.Error) "Passing cases failed: $($pass.Error)"
Assert-Check (@($pass.Manifest | Where-Object Status -ne 'Passed').Count -eq 0) 'A passing case had the wrong status.'
Assert-Check (@(Get-ChildItem -LiteralPath $pass.Output -Filter '*.bin').Count -eq 0) 'Passing inputs were retained.'
# Baseline hashes from the original coordinator, with sample.mdb = bytes 0..255 and seed 42.
$expectedHashes = @(
    '40AFF2E9D2D8922E47AFD4648E6967497158785FBD1DA870E7110266BF944880'
    'F2B51A1A5C12E9B07F152812895F2AB51A9727021E389555A58507EA7FF16E51'
    '3D8AB6846C5E4D6D932145900C6E5416E11231A827514A9B9D696DC95CBF0CE1'
    'BDCEE56D40BAD202602BC5CE820225539D8F79F0EBE5A60475A7EB63ABB914B9'
    '99B2DAD1913130FBDED67B34A12C7DCBC3643AE284F49C27E6712F6925B3D7E0'
    'EFDD2ABE671E17B0B6CDB629CF2016532A6FD3DA56C3DB605E345C0E193A5288'
    '02E594F33B148FC3B5ED7D3D36A6E682C2F2602B168E64CED94B5644E91E87F4'
    '33C00BA838F672062F91323FB2A740362F3A1692C7E5110425AEF565C05E15C9'
    '5FC7E2A823B847867E65236CE314BD8CB8A50BA516E8D62C439D36CC14C6285E'
    '0CA410556E8661A36CC8E37BC0A02F0DECEB589495EB349B70D6A13E345BA01B'
    'ED68E5A7ABCBD6E9E802039A2ECF372881F2F00D2A2F5404C4C7AD094945AF2F'
    'A88B937A8681536524DAB3614B692C7EFCF3AEC9B82993B090B75CA64F274847'
)
Assert-Check (($pass.Manifest.SHA256 -join ',') -eq ($expectedHashes -join ',')) 'Generated inputs changed from the baseline.'
$repeat = Invoke-Scenario 'repeat' 'Pass' 4
Assert-Check (($pass.Manifest.SHA256 -join ',') -eq ($repeat.Manifest.SHA256 -join ',')) 'The seed did not reproduce the same input hashes.'
$missing = Invoke-Scenario 'stale-results' 'Missing' 1
Assert-Check ($null -ne $missing.Error) 'Stale XML results falsely passed.'
Assert-Check (@($missing.Manifest | Where-Object Status -eq 'MissingResult').Count -eq 3) 'Missing current results were not recorded.'
Assert-Check (@(Get-ChildItem -LiteralPath $missing.Output -Filter '*.bin').Count -eq 3) 'Missing-result input evidence was deleted.'
$malformed = Invoke-Scenario 'malformed-results' 'Malformed' 1
Assert-Check ($null -ne $malformed.Error) 'Malformed XML results falsely passed.'
Assert-Check (@($malformed.Manifest | Where-Object Status -eq 'InvalidResult').Count -eq 3) 'Malformed results were not recorded.'
Assert-Check (@(Get-ChildItem -LiteralPath $malformed.Output -Filter '*.bin').Count -eq 3) 'Malformed-result input evidence was not retained.'
foreach ($mode in @('Failed', 'Unexpected', 'MemoryLimit', 'Timeout')) {
    $run = Invoke-Scenario $mode $mode 1
    $expectedStatus = if ($mode -eq 'Unexpected') { 'UnexpectedResult' } else { $mode }
    Assert-Check ($null -ne $run.Error) "$mode cases falsely passed."
    Assert-Check (@($run.Manifest | Where-Object Status -eq $expectedStatus).Count -eq 3) "$mode cases had the wrong status."
    Assert-Check (@(Get-ChildItem -LiteralPath $run.Output -Filter '*.bin').Count -eq 3) "$mode input evidence was deleted."
}
Write-Host "Mocked fuzz artifacts: $testRoot"
if ($fuzzChecks.Count -gt 0) { throw ($fuzzChecks -join [Environment]::NewLine) }
Write-Host 'All mocked hosted fuzz checks passed.'
