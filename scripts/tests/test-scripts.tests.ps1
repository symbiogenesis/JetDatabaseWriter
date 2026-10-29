# Exercise the regression coordinator with tiny independent subprocess fixtures.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('jdw-script-coordinator-' + [guid]::NewGuid().ToString('N'))
$pwsh = Join-Path $PSHOME $(if ($IsWindows) { 'pwsh.exe' } else { 'pwsh' })

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function New-Scenario([string] $Name) {
    $root = Join-Path $fixture $Name
    [IO.Directory]::CreateDirectory((Join-Path $root 'tests')) | Out-Null
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../test-scripts.ps1') -Destination $root
    return $root
}

function Invoke-Coordinator([string] $Root) {
    $output = & $pwsh -NoProfile -File (Join-Path $Root 'test-scripts.ps1') 2>&1
    return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $output -join "`n" }
}

$parallel = New-Scenario 'parallel'
$rendezvous = @'
$root = Split-Path -Parent $PSScriptRoot
$name = [IO.Path]::GetFileNameWithoutExtension($PSCommandPath)
[IO.File]::WriteAllText((Join-Path $root ($name + '.ready')), 'ready')
$clock = [Diagnostics.Stopwatch]::StartNew()
while (@(Get-ChildItem -LiteralPath $root -Filter '*.ready' -File).Count -ne 2) {
    if ($clock.Elapsed.TotalSeconds -gt 5) { throw 'Independent regression checks were serialized.' }
    Start-Sleep -Milliseconds 20
}
Write-Host "$name passed."
'@
foreach ($name in 'first', 'second') {
    [IO.File]::WriteAllText((Join-Path $parallel "tests/$name.tests.ps1"), $rendezvous)
}
$result = Invoke-Coordinator $parallel
Assert-Check ($result.ExitCode -eq 0) "Independent regression checks must overlap: $($result.Output)"
Assert-Check ($result.Output.Contains('first.tests passed.') -and $result.Output.Contains('second.tests passed.')) 'The coordinator lost child diagnostics.'

$failure = New-Scenario 'failure'
[IO.File]::WriteAllText((Join-Path $failure 'tests/first.tests.ps1'), "Write-Host 'Failed child diagnostics'; exit 7")
[IO.File]::WriteAllText((Join-Path $failure 'tests/second.tests.ps1'), "Write-Host 'Passing child diagnostics'")
$result = Invoke-Coordinator $failure
Assert-Check ($result.ExitCode -ne 0) 'A failed regression subprocess passed the coordinator.'
Assert-Check ($result.Output.Contains('first.tests.ps1') -and $result.Output.Contains('7')) 'The failed test and exit code were lost.'
Assert-Check ($result.Output.Contains('Failed child diagnostics') -and $result.Output.Contains('Passing child diagnostics')) 'Parallel child diagnostics were not retained on failure.'

$parse = New-Scenario 'parse'
[IO.File]::WriteAllText((Join-Path $parse 'invalid.ps1'), 'function {')
[IO.File]::WriteAllText((Join-Path $parse 'tests/first.tests.ps1'), "Write-Host 'This must not execute'")
$result = Invoke-Coordinator $parse
Assert-Check ($result.ExitCode -ne 0 -and $result.Output.Contains('invalid.ps1')) 'The coordinator accepted a parse error.'
Assert-Check (-not $result.Output.Contains('This must not execute')) 'Regression subprocesses started before parsing completed.'
Write-Host "Script coordinator concurrency, failure and parse checks passed. Artifacts: $fixture"
