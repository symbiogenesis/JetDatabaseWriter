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
$context = @{ Calls = [Collections.Generic.List[object]]::new(); FailAt = 0; Version = '10.0.402'; MissingNative = $false; EmptyLibrary = $false }

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function New-Fixture {
    $context.Calls.Clear()
    $context.FailAt = 0
    $context.Version = '10.0.402'
    $context.MissingNative = $false
    $context.EmptyLibrary = $false
    [IO.Directory]::CreateDirectory((Join-Path $fixture 'src/Library')) | Out-Null
    [IO.Directory]::CreateDirectory((Join-Path $scripts 'AotSmoke')) | Out-Null
    [IO.Directory]::CreateDirectory((Join-Path $fixture '.github/dotnet11')) | Out-Null
    [IO.File]::WriteAllText((Join-Path $fixture '.github/dotnet11/global.json'), "{`r`n  `"sdk`": { `"version`": `"11.0.100-rc.2.26501.1`", `"rollForward`": `"disable`", `"allowPrerelease`": false }`r`n}`r`n")
    [IO.File]::WriteAllText((Join-Path $fixture 'global.json'), "{`r`n  `"sdk`": { `"version`": `"10.0.401`", `"rollForward`": `"disable`" }`r`n}`r`n")
    [IO.File]::WriteAllText((Join-Path $fixture 'JetDatabaseWriter.slnx'), '<Solution><Folder Name="/src/"><Project Path="src/Library/Library.csproj" /></Folder></Solution>')
    [IO.File]::WriteAllText((Join-Path $fixture 'src/Library/packages.lock.json'), "original library`r`n")
    [IO.File]::WriteAllText((Join-Path $fixture 'src/Library/packages.netstandard.lock.json'), "original standard`r`n")
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
        } elseif ($arguments -contains '-p:TestNetStandard=true') {
            Join-Path $fixture 'src/Library/packages.netstandard.lock.json'
        } else { Join-Path $fixture 'src/Library/packages.lock.json' }
        [IO.File]::WriteAllText($lock, "updated $($context.Version)`n")
    }
}

function Invoke-Scenario([string] $Name, [string] $ExpectedError = '', [string] $Version = '10.0.402') {
    $location = (Get-Location).Path
    $originalConfiguration = [IO.File]::ReadAllText((Join-Path $fixture 'global.json'))
    $originalRuntimeConfiguration = [IO.File]::ReadAllText((Join-Path $fixture '.github/dotnet11/global.json'))
    $caught = ''
    try { & (Join-Path $scripts 'update-sdk.ps1') -Version $Version }
    catch { $caught = $_.Exception.Message }
    Assert-Check ((Get-Location).Path -eq $location) "$Name did not restore the caller's directory."
    if ($ExpectedError) {
        Assert-Check ($caught.Contains($ExpectedError)) "$Name expected '$ExpectedError', got '$caught'."
        Assert-Check (([IO.File]::ReadAllText((Join-Path $fixture 'global.json')) | ConvertFrom-Json).sdk.version -eq '10.0.401') "$Name kept a partial SDK update."
        Assert-Check ([IO.File]::ReadAllText((Join-Path $fixture 'global.json')) -ceq $originalConfiguration) "$Name did not restore the exact SDK configuration bytes."
        Assert-Check ([IO.File]::ReadAllText((Join-Path $fixture '.github/dotnet11/global.json')) -ceq $originalRuntimeConfiguration) "$Name did not restore the exact CI runtime configuration bytes."
        $libraryLock = Join-Path $fixture 'src/Library/packages.lock.json'
        Assert-Check ([IO.File]::Exists($libraryLock)) "$Name deleted an existing library lock."
        $expectedLibrary = if ($context.EmptyLibrary) { '' } else { "original library`r`n" }
        Assert-Check ([IO.File]::ReadAllText($libraryLock) -eq $expectedLibrary) "$Name did not restore the original library lock bytes."
        Assert-Check ([IO.File]::ReadAllText((Join-Path $fixture 'src/Library/packages.netstandard.lock.json')) -eq "original standard`r`n") "$Name did not restore the standard-asset lock bytes."
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
Assert-Check ($context.Calls.Count -eq 5) 'Locked restoration must cover the solution and all three smoke modes.'
Assert-Check (($context.Calls | ForEach-Object { $_ -join ' ' })[0] -like '*JetDatabaseWriter.slnx*') 'The solution restore is missing.'
foreach ($call in $context.Calls) {
    Assert-Check ($call -contains '--locked-mode' -and $call -notcontains '--force-evaluate') 'Ordinary restoration must remain locked.'
}
foreach ($mode in 'seed', 'trimmed', 'native') {
    Assert-Check (@($context.Calls | Where-Object { $_ -contains "-p:SmokeMode=$mode" }).Count -eq 1) "The $mode lock was not restored exactly once."
}
Assert-Check (@($context.Calls | Where-Object { $_ -contains '-p:TestNetStandard=true' }).Count -eq 1) 'The standard-asset lock must be restored exactly once.'
'PASS: locked mode covers solution, standard asset and every smoke configuration'

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
Assert-Check ($context.Calls.Count -eq 10) 'SDK updates must refresh and then verify all five restore targets.'
for ($i = 0; $i -lt 5; $i++) {
    Assert-Check ($context.Calls[$i] -contains '--force-evaluate' -and $context.Calls[$i] -contains '-p:RestoreLockedMode=false') 'Refresh did not explicitly unlock restoration.'
    Assert-Check ($context.Calls[$i + 5] -contains '--locked-mode') 'Fresh locks were not verified in locked mode.'
}
Assert-Check (([IO.File]::ReadAllText((Join-Path $fixture 'global.json')) | ConvertFrom-Json).sdk.version -eq '10.0.402') 'The SDK pin was not updated.'
Assert-Check (([IO.File]::ReadAllText((Join-Path $fixture '.github/dotnet11/global.json')) | ConvertFrom-Json).sdk.version -eq '11.0.100-rc.2.26501.1') 'Updating .NET 10 must preserve the separate .NET 11 runtime pin.'
Assert-Check ([IO.File]::ReadAllText((Join-Path $scripts 'AotSmoke/packages.native.lock.json')) -eq "updated 10.0.402`r`n") 'Generated locks were not normalized to CRLF.'

Assert-Check ([IO.File]::ReadAllText((Join-Path $fixture 'src/Library/packages.netstandard.lock.json')) -eq "updated 10.0.402`r`n") 'The standard-asset lock was not refreshed and normalized.'

foreach ($stage in 1, 2, 3, 4, 5, 6, 10) {
    New-Fixture
    $context.FailAt = $stage
    Invoke-Scenario "restore failure $stage rolls back" 'failed with exit code 7'
    Assert-Check ($context.Calls.Count -eq $stage) 'The updater continued restoring after a failure.'
}
New-Fixture
$context.EmptyLibrary = $true
[IO.File]::WriteAllBytes((Join-Path $fixture 'src/Library/packages.lock.json'), [byte[]]@())
$context.FailAt = 2
Invoke-Scenario 'existing empty locks survive rollback' 'failed with exit code 7'
New-Fixture
$context.MissingNative = $true
[IO.File]::Delete((Join-Path $scripts 'AotSmoke/packages.native.lock.json'))
$context.FailAt = 5
Invoke-Scenario 'newly created locks roll back' 'failed with exit code 7'
foreach ($version in '11.0.100', '11.0.100-rc.3.26522.148') {
    New-Fixture
    $context.Version = $version
    Invoke-Scenario "exact SDK $version refreshes all locks" -Version $version
    $updated = [IO.File]::ReadAllText((Join-Path $fixture 'global.json')) | ConvertFrom-Json
    Assert-Check ($updated.sdk.version -eq $version) 'The exact requested SDK pin was lost.'
    $runtime = [IO.File]::ReadAllText((Join-Path $fixture '.github/dotnet11/global.json')) | ConvertFrom-Json
    Assert-Check ($runtime.sdk.version -eq $version) 'The CI runtime SDK pin diverged from the requested .NET 11 SDK.'
    Assert-Check ($context.Calls.Count -eq 10) 'The requested SDK did not refresh and verify every lock.'
    if ($version.Contains('-')) {
        Assert-Check ($updated.sdk.allowPrerelease -eq $true -and $runtime.sdk.allowPrerelease -eq $true) 'Both prerelease pins must explicitly allow their SDK.'
    }
}
foreach ($allowPrerelease in 'false', 'true') {
    New-Fixture
    $configurationPath = Join-Path $fixture 'global.json'
    [IO.File]::WriteAllText($configurationPath, [IO.File]::ReadAllText($configurationPath).Replace('"rollForward"', ('"allowPrerelease": ' + $allowPrerelease + ', "rollForward"')))
    $context.Version = '11.0.100-rc.3.26522.148'
    Invoke-Scenario "prerelease enables existing setting $allowPrerelease" -Version $context.Version
    Assert-Check (([IO.File]::ReadAllText($configurationPath) | ConvertFrom-Json).sdk.allowPrerelease -eq $true) 'The prerelease SDK remains disabled.'
}
New-Fixture
$context.Version = '11.0.100-rc.3.26522.148'
$context.FailAt = 2
Invoke-Scenario 'prerelease settings and locks roll back together' 'failed with exit code 7' -Version $context.Version
foreach ($invalid in '11.0.x', '12.0.100', '11.0.100-rc.3', '11.0.100-rc.*', '11.0.100-rc.3.26522', '10.0.100-rc.3.26522.148', "11.0.100`n") {
    New-Fixture
    Invoke-Scenario "invalid exact SDK '$invalid' is refused before restoring" 'pattern' -Version $invalid
    Assert-Check ($context.Calls.Count -eq 0) 'An invalid SDK reached restoration.'
}
New-Fixture
$context.Version = '10.0.401'
Invoke-Scenario 'missing requested SDK rolls back' 'selected SDK'
Assert-Check ($context.Calls.Count -eq 0) 'A mismatched SDK must be refused before restoring.'
Write-Host "All mocked SDK checks passed. Artifacts: $testRoot"
