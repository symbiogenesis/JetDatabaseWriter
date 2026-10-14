# Exercise SDK updates and every restore mode with command doubles; no builds or network calls run.
[CmdletBinding()]
param([string] $OutputDirectory)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('jdw-sdk-tests-' + [Guid]::NewGuid().ToString('N'))
}
$testRoot = [IO.Path]::GetFullPath($OutputDirectory)
$fixture = Join-Path $testRoot 'fixture'
$scripts = Join-Path $fixture 'scripts'
[IO.Directory]::CreateDirectory($scripts) | Out-Null
foreach ($name in 'restore-dependencies.ps1', 'update-sdk.ps1') {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot ('../' + $name)) -Destination (Join-Path $scripts $name)
}
$context = @{ Calls = [Collections.Generic.List[object]]::new(); FailAt = 0; Version = '10.0.402'; MissingNative = $false }

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function New-Fixture {
    $context.Calls.Clear()
    $context.FailAt = 0
    $context.Version = '10.0.402'
    $context.MissingNative = $false
    [IO.Directory]::CreateDirectory((Join-Path $fixture 'Library')) | Out-Null
    [IO.Directory]::CreateDirectory((Join-Path $scripts 'AotSmoke')) | Out-Null
    [IO.File]::WriteAllText((Join-Path $fixture 'global.json'), "{`r`n  `"sdk`": { `"version`": `"10.0.401`", `"rollForward`": `"disable`" }`r`n}`r`n")
    [IO.File]::WriteAllText((Join-Path $fixture 'JetDatabaseWriter.slnx'), '<Solution><Project Path="Library/Library.csproj" /></Solution>')
    [IO.File]::WriteAllText((Join-Path $fixture 'Library/packages.lock.json'), "original library`r`n")
    [IO.File]::WriteAllText((Join-Path $scripts 'AotSmoke/AotSmoke.csproj'), '<Project />')
    foreach ($mode in 'seed', 'trimmed', 'native') {
        [IO.File]::WriteAllText((Join-Path $scripts ("AotSmoke/packages.$mode.lock.json")), "original $mode`r`n")
    }
}

function dotnet {
    $arguments = [string[]]$args
    $global:LASTEXITCODE = 0
    if ($arguments[0] -eq '--version') { $context.Version; return }
    Assert-Check ($arguments[0] -eq 'restore') 'The updater must never build or run a project.'
    $context.Calls.Add($arguments)
    if ($context.FailAt -eq $context.Calls.Count) { $global:LASTEXITCODE = 7; return }
    if ($arguments -contains '--force-evaluate') {
        $mode = @($arguments | Where-Object { $_ -like '-p:SmokeMode=*' })
        $lock = if ($mode.Count) {
            Join-Path $scripts ('AotSmoke/packages.' + $mode[0].Split('=')[1] + '.lock.json')
        } else { Join-Path $fixture 'Library/packages.lock.json' }
        [IO.File]::WriteAllText($lock, "updated $($context.Version)`n")
    }
}

function Invoke-Scenario([string] $Name, [string] $ExpectedError = '') {
    $location = (Get-Location).Path
    $caught = ''
    try { & (Join-Path $scripts 'update-sdk.ps1') -Version '10.0.402' }
    catch { $caught = $_.Exception.Message }
    Assert-Check ((Get-Location).Path -eq $location) "$Name did not restore the caller's directory."
    if ($ExpectedError) {
        Assert-Check ($caught.Contains($ExpectedError)) "$Name expected '$ExpectedError', got '$caught'."
        Assert-Check (([IO.File]::ReadAllText((Join-Path $fixture 'global.json')) | ConvertFrom-Json).sdk.version -eq '10.0.401') "$Name kept a partial SDK update."
        Assert-Check ([IO.File]::ReadAllText((Join-Path $fixture 'Library/packages.lock.json')) -eq "original library`r`n") "$Name did not restore the original library lock bytes."
        foreach ($mode in 'seed', 'trimmed', 'native') {
            $path = Join-Path $scripts ("AotSmoke/packages.$mode.lock.json")
            if ($mode -eq 'native' -and $context.MissingNative) {
                Assert-Check (-not [IO.File]::Exists($path)) "$Name retained a newly created lock after failure."
            } else {
                Assert-Check ([IO.File]::ReadAllText($path) -eq "original $mode`r`n") "$Name did not restore the original $mode lock bytes."
            }
        }
    } else { Assert-Check (-not $caught) "$Name failed: $caught" }
    "PASS: $Name"
}

New-Fixture
& (Join-Path $scripts 'restore-dependencies.ps1')
Assert-Check ($context.Calls.Count -eq 4) 'Locked restoration must cover the solution and all three smoke modes.'
Assert-Check (($context.Calls | ForEach-Object { $_ -join ' ' })[0] -like '*JetDatabaseWriter.slnx*') 'The solution restore is missing.'
foreach ($call in $context.Calls) {
    Assert-Check ($call -contains '--locked-mode' -and $call -notcontains '--force-evaluate') 'Ordinary restoration must remain locked.'
}
foreach ($mode in 'seed', 'trimmed', 'native') {
    Assert-Check (@($context.Calls | Where-Object { $_ -contains "-p:SmokeMode=$mode" }).Count -eq 1) "The $mode lock was not restored exactly once."
}
'PASS: locked mode covers solution and every smoke configuration'

New-Fixture
& (Join-Path $scripts 'restore-dependencies.ps1') -AotOnly
Assert-Check ($context.Calls.Count -eq 3) 'AOT preflight must restore every smoke mode without restoring the solution.'
$actualModes = @($context.Calls | ForEach-Object { @($_ | Where-Object { $_ -like '-p:SmokeMode=*' })[0].Split('=')[1] } | Sort-Object -Unique)
$project = [xml][IO.File]::ReadAllText((Join-Path $PSScriptRoot '../AotSmoke/AotSmoke.csproj'))
$configuredModes = @($project.SelectNodes('//*[@Condition]') | Where-Object { $_.Condition.Contains('$(SmokeMode)') } | ForEach-Object {
    [regex]::Match($_.Condition, "==\s*'(?<mode>[^']+)'").Groups['mode'].Value
} | Sort-Object -Unique)
Assert-Check (($actualModes -join ',') -eq ($configuredModes -join ',')) 'Every configured smoke mode must be covered by restore preflight.'
$lockModes = @(Get-ChildItem -LiteralPath (Join-Path $PSScriptRoot '../AotSmoke') -Filter 'packages.*.lock.json' -File | ForEach-Object { $_.Name.Split('.')[1] } | Sort-Object -Unique)
Assert-Check (($actualModes -join ',') -eq ($lockModes -join ',')) 'Every smoke lockfile must be covered by restore preflight.'
'PASS: AOT-only preflight'

New-Fixture
Invoke-Scenario 'complete atomic SDK update'
Assert-Check ($context.Calls.Count -eq 8) 'SDK updates must refresh and then verify all four restore targets.'
for ($i = 0; $i -lt 4; $i++) {
    Assert-Check ($context.Calls[$i] -contains '--force-evaluate' -and $context.Calls[$i] -contains '-p:RestoreLockedMode=false') 'Refresh did not explicitly unlock restoration.'
    Assert-Check ($context.Calls[$i + 4] -contains '--locked-mode') 'Fresh locks were not verified in locked mode.'
}
Assert-Check (([IO.File]::ReadAllText((Join-Path $fixture 'global.json')) | ConvertFrom-Json).sdk.version -eq '10.0.402') 'The SDK pin was not updated.'
Assert-Check ([IO.File]::ReadAllText((Join-Path $scripts 'AotSmoke/packages.native.lock.json')) -eq "updated 10.0.402`r`n") 'Generated locks were not normalized to CRLF.'

foreach ($stage in 1, 2, 3, 4, 5, 8) {
    New-Fixture
    $context.FailAt = $stage
    Invoke-Scenario "restore failure $stage rolls back" 'failed with exit code 7'
    Assert-Check ($context.Calls.Count -eq $stage) 'The updater continued restoring after a failure.'
}
New-Fixture
$context.MissingNative = $true
[IO.File]::Delete((Join-Path $scripts 'AotSmoke/packages.native.lock.json'))
$context.FailAt = 5
Invoke-Scenario 'newly created locks roll back' 'failed with exit code 7'
New-Fixture
$context.Version = '10.0.401'
Invoke-Scenario 'missing requested SDK rolls back' 'selected SDK'
Assert-Check ($context.Calls.Count -eq 0) 'A mismatched SDK must be refused before restoring.'
Write-Host "All mocked SDK checks passed. Artifacts: $testRoot"
