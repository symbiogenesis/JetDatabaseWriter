# Exercise archive refusal and consumer coordination without builds or package feeds.
[CmdletBinding()]
param([string] $OutputDirectory)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $OutputDirectory) { $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-test-package-' + [Guid]::NewGuid().ToString('N')) }
$testRoot = [IO.Path]::GetFullPath($OutputDirectory)
[IO.Directory]::CreateDirectory($testRoot) | Out-Null
$subject = Join-Path $PSScriptRoot '../test-package.ps1'
$packageTestContext = [pscustomobject]@{ Calls = [Collections.Generic.List[object]]::new(); Failure = ''; MissingRuntime = ''; FailureConsumer = '' }

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function New-Package([string] $Name, [string] $Missing = '', [string] $Stale = '') {
    $directory = Join-Path $testRoot $Name
    [IO.Directory]::CreateDirectory($directory) | Out-Null
    $archive = [IO.Compression.ZipFile]::Open((Join-Path $directory 'JetDatabaseWriter.1.0.0.nupkg'), [IO.Compression.ZipArchiveMode]::Create)
    try {
        $entries = [ordered]@{
            'JetDatabaseWriter.nuspec' = '<package><metadata><id>JetDatabaseWriter</id><version>1.0.0</version><projectUrl>https://symbiogenesis.github.io/JetDatabaseWriter/</projectUrl></metadata></package>'
            'lib/net11.0/JetDatabaseWriter.dll' = 'Synthetic DLL'
            'lib/net11.0/JetDatabaseWriter.xml' = 'Synthetic documentation'
            'lib/netstandard2.1/JetDatabaseWriter.dll' = 'Synthetic DLL'
            'lib/netstandard2.1/JetDatabaseWriter.xml' = 'Synthetic documentation'
        }
        foreach ($file in 'README.md', 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'THIRD-PARTY-NOTICES.md') { $entries[$file] = [IO.File]::ReadAllText((Join-Path $repository $file)) }
        foreach ($name in $entries.Keys) {
            if ($name -eq $Missing) { continue }
            $writer = [IO.StreamWriter]::new($archive.CreateEntry($name).Open())
            try { $writer.Write($(if ($name -eq $Stale) { if ($name -eq 'JetDatabaseWriter.nuspec') { '<package><metadata><id>JetDatabaseWriter</id><version>1.0.0</version><projectUrl>https://github.com/symbiogenesis/JetDatabaseWriter</projectUrl></metadata></package>' } else { 'Incomplete license' } } else { $entries[$name] })) } finally { $writer.Dispose() }
        }
    }
    finally { $archive.Dispose() }
    $efArchive = [IO.Compression.ZipFile]::Open((Join-Path $directory 'JetDatabaseWriter.EntityFrameworkCore.1.0.0.nupkg'), [IO.Compression.ZipArchiveMode]::Create)
    try {
        $efEntries = [ordered]@{
            'JetDatabaseWriter.EntityFrameworkCore.nuspec' = '<package><metadata><id>JetDatabaseWriter.EntityFrameworkCore</id><version>1.0.0</version><projectUrl>https://symbiogenesis.github.io/JetDatabaseWriter/</projectUrl></metadata></package>'
            'lib/net11.0/JetDatabaseWriter.EntityFrameworkCore.dll' = 'Synthetic DLL'
            'lib/net11.0/JetDatabaseWriter.EntityFrameworkCore.xml' = 'Synthetic documentation'
            'lib/net10.0/JetDatabaseWriter.EntityFrameworkCore.dll' = 'Synthetic DLL'
            'lib/net10.0/JetDatabaseWriter.EntityFrameworkCore.xml' = 'Synthetic documentation'
        }
        foreach ($file in 'README.md', 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'THIRD-PARTY-NOTICES.md') { $efEntries[$file] = [IO.File]::ReadAllText((Join-Path $repository $file)) }
        foreach ($name in $efEntries.Keys) {
            if ($name -eq $Missing) { continue }
            $writer = [IO.StreamWriter]::new($efArchive.CreateEntry($name).Open())
            try { $writer.Write($(if ($name -eq $Stale) { if ($name -eq 'JetDatabaseWriter.EntityFrameworkCore.nuspec') { '<package><metadata><id>JetDatabaseWriter.EntityFrameworkCore</id><version>1.0.0</version><projectUrl>https://github.com/symbiogenesis/JetDatabaseWriter</projectUrl></metadata></package>' } else { 'Incomplete license' } } else { $efEntries[$name] })) } finally { $writer.Dispose() }
        }
    }
    finally { $efArchive.Dispose() }
    return $directory
}

function dotnet {
    $arguments = [string[]]$args
    $packageTestContext.Calls.Add($arguments)
    $stage = $arguments[0]
    Assert-Check ($stage -in '--list-runtimes', 'restore', 'build', 'exec') "Unexpected dotnet operation: $stage"
    $global:LASTEXITCODE = if ($packageTestContext.Failure -eq $stage) { 17 } else { 0 }
    if ($stage -eq 'exec' -and $packageTestContext.FailureConsumer -and ($arguments -join '/') -like ('*' + $packageTestContext.FailureConsumer + '*')) {
        $global:LASTEXITCODE = 17
    }
    if ($stage -eq '--list-runtimes') {
        if ($packageTestContext.MissingRuntime -ne '10') { 'Microsoft.NETCore.App 10.0.14 [synthetic/runtimes]' }
        if ($packageTestContext.MissingRuntime -ne '11') { 'Microsoft.NETCore.App 11.0.0-rc.3.26522.148 [synthetic/runtimes]' }
    }
}

function Invoke-Scenario([string] $Name, [string] $PackageDirectory, [string] $Failure = '') {
    $packageTestContext.Calls.Clear()
    $packageTestContext.Failure = $Failure
    $output = Join-Path $testRoot ($Name + '-consumer')
    $location = Get-Location
    $caught = ''
    try { & $subject -PackageDirectory $PackageDirectory -OutputDirectory $output }
    catch { $caught = $_.Exception.Message }
    Assert-Check ((Get-Location).Path -eq $location.Path) "$Name did not restore the caller's working directory."
    return [pscustomobject]@{ Output = $output; Error = $caught }
}

$valid = New-Package 'valid'
$pass = Invoke-Scenario 'pass' $valid
Assert-Check (-not $pass.Error) "Valid package failed: $($pass.Error)"
Assert-Check ($packageTestContext.Calls.Count -eq 13) 'Runtime discovery and all four consumers must complete.'
Assert-Check (($packageTestContext.Calls | ForEach-Object { $_[0] }) -join ',' -eq '--list-runtimes,restore,build,exec,restore,build,exec,restore,build,exec,restore,build,exec') 'Consumer validation changed its operation sequence.'
foreach ($consumer in 'net11.0', 'net8.0', 'ef-net11.0', 'ef-net10.0') {
    $target = $consumer.Replace('ef-', '')
    $project = [xml][IO.File]::ReadAllText((Join-Path $pass.Output "$consumer/PackageConsumer.csproj"))
    Assert-Check ($project.Project.PropertyGroup.TargetFramework -eq $target) 'The consumer framework was lost.'
    Assert-Check ($project.Project.ItemGroup.PackageReference.Version -eq '[1.0.0]') 'The consumer must restore the exact artifact version.'
    $references = @($project.SelectNodes('//PackageReference'))
    $packageId = if ($consumer.StartsWith('ef-')) { 'JetDatabaseWriter.EntityFrameworkCore' } else { 'JetDatabaseWriter' }
    Assert-Check ($references.Count -eq 1 -and $references[0].Include -ceq $packageId) 'Each consumer must reference only its shipping package, exercising transitive dependencies.'
    Assert-Check ($project.SelectNodes('//ProjectReference').Count -eq 0) 'The package consumer must not compile a repository project.'
    $dll = Join-Path $pass.Output "$consumer/bin/Release/$target/PackageConsumer.dll"
    $execution = @($packageTestContext.Calls | Where-Object { $_[0] -eq 'exec' -and $_ -contains $dll })
    $major = if ($target -eq 'net11.0') { '11' } else { '10' }
    $coreTarget = if ($major -eq '11') { '.NETCoreApp,Version=v11.0' } else { '.NETStandard,Version=v2.1' }
    Assert-Check ($execution.Count -eq 1 -and $execution[0] -contains $coreTarget -and $execution[0] -contains $major) 'Each consumer must verify its loaded core asset and runtime.'
    if ($consumer.StartsWith('ef-')) {
        Assert-Check ($execution[0][-1] -ceq ".NETCoreApp,Version=v$major.0") 'Each EF consumer must verify its provider asset.'
    }
}
$config = [xml][IO.File]::ReadAllText((Join-Path $pass.Output 'NuGet.Config'))
foreach ($packageId in 'JetDatabaseWriter', 'JetDatabaseWriter.EntityFrameworkCore') {
    Assert-Check ($null -ne $config.SelectSingleNode("/configuration/packageSourceMapping/packageSource[@key='artifact']/package[@pattern='$packageId']")) 'Both shipping packages must come from the isolated artifact source.'
}
foreach ($name in 'Directory.Build.props', 'Directory.Build.targets', 'Directory.Packages.props') { Assert-Check ([IO.File]::ReadAllText((Join-Path $pass.Output $name)) -eq '<Project />') 'The consumer inherited enclosing repository build configuration.' }
Assert-Check ([IO.File]::ReadAllText((Join-Path $pass.Output '.editorconfig')) -eq 'root = true') 'The consumer inherited repository analyzer configuration.'
Assert-Check (@($packageTestContext.Calls | Where-Object { $_[0] -eq 'restore' -and $_ -contains '--no-http-cache' -and $_ -contains (Join-Path $pass.Output 'packages') }).Count -eq 4) 'All four consumers require isolated restores.'
Assert-Check ($packageTestContext.Calls[3][-2] -eq '.NETCoreApp,Version=v11.0' -and $packageTestContext.Calls[6][-2] -eq '.NETStandard,Version=v2.1') 'Each consumer must verify its loaded library asset.'
Assert-Check ($packageTestContext.Calls[3][-1] -eq '11' -and $packageTestContext.Calls[6][-1] -eq '10') 'Each consumer must verify its actual runtime major.'
foreach ($case in @(@{ Index = 3; Runtime = '11.0.0-rc.3.26522.148' }, @{ Index = 6; Runtime = '10.0.14' }, @{ Index = 9; Runtime = '11.0.0-rc.3.26522.148' }, @{ Index = 12; Runtime = '10.0.14' })) {
    $arguments = $packageTestContext.Calls[$case.Index]
    $option = [Array]::IndexOf($arguments, '--fx-version')
    Assert-Check ($option -ge 0 -and $arguments[$option + 1] -ceq $case.Runtime) 'Package consumers must pin the intended installed runtime, including prereleases.'
}

foreach ($missing in 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'lib/netstandard2.1/JetDatabaseWriter.dll', 'lib/net11.0/JetDatabaseWriter.xml') {
    $name = 'missing-' + [IO.Path]::GetFileName($missing)
    $scenario = Invoke-Scenario $name (New-Package $name $missing)
    Assert-Check ($scenario.Error.Contains('missing ' + $missing.Split('/')[-1]) -or $scenario.Error.Contains('missing the ')) "Missing $missing was accepted: $($scenario.Error)"
    Assert-Check ($packageTestContext.Calls.Count -eq 0) 'A malformed archive reached the consumer commands.'
}
$stale = Invoke-Scenario 'stale' (New-Package 'stale' '' 'LICENSE-APACHE-2.0.txt')
Assert-Check ($stale.Error.Contains('stale or incomplete LICENSE-APACHE-2.0.txt')) 'An incomplete Apache license was accepted.'
$staleProjectUrl = Invoke-Scenario 'stale-project-url' (New-Package 'stale-project-url' '' 'JetDatabaseWriter.nuspec')
Assert-Check ($staleProjectUrl.Error.Contains('Invalid shipping package project URL.')) 'A stale package project URL was accepted.'
Assert-Check ($packageTestContext.Calls.Count -eq 0) 'A stale package project URL reached the consumer commands.'
foreach ($stage in '--list-runtimes', 'restore', 'build', 'exec') {
    $scenario = Invoke-Scenario ('failed-' + $stage) $valid $stage
    Assert-Check ($scenario.Error.Contains('failed with exit code 17')) "$stage failure was not propagated."
    Assert-Check ($packageTestContext.Calls[-1][0] -eq $stage) "$stage failure did not stop subsequent work."
}
foreach ($major in '10', '11') {
    $packageTestContext.MissingRuntime = $major
    $scenario = Invoke-Scenario ('missing-runtime-' + $major) $valid
    Assert-Check ($scenario.Error.Contains("requires an installed .NET $major runtime")) 'A missing runtime was silently replaced by another major.'
}
$packageTestContext.MissingRuntime = ''
foreach ($consumer in 'ef-net11.0', 'ef-net10.0') {
    $packageTestContext.FailureConsumer = $consumer
    $scenario = Invoke-Scenario ('failed-' + $consumer) $valid
    Assert-Check ($scenario.Error.Contains('failed with exit code 17')) 'An EF consumer failure must fail package validation.'
    Assert-Check (($packageTestContext.Calls[-1] -join '/') -like ('*' + $consumer + '*')) 'An EF consumer failure did not stop subsequent work.'
}
Write-Host "All package coordinator and malformed archive checks passed. Artifacts: $testRoot"
