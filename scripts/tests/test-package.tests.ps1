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
$packageTestContext = [pscustomobject]@{ Calls = [Collections.Generic.List[object]]::new(); Failure = '' }

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function New-Package([string] $Name, [string] $Missing = '', [string] $Stale = '') {
    $directory = Join-Path $testRoot $Name
    [IO.Directory]::CreateDirectory($directory) | Out-Null
    $archive = [IO.Compression.ZipFile]::Open((Join-Path $directory 'JetDatabaseWriter.4.0.0-alpha.1.nupkg'), [IO.Compression.ZipArchiveMode]::Create)
    try {
        $entries = [ordered]@{
            'JetDatabaseWriter.nuspec' = '<package><metadata><id>JetDatabaseWriter</id><version>4.0.0-alpha.1</version></metadata></package>'
            'lib/net10.0/JetDatabaseWriter.dll' = 'Synthetic DLL'
            'lib/net10.0/JetDatabaseWriter.xml' = 'Synthetic documentation'
            'lib/netstandard2.1/JetDatabaseWriter.dll' = 'Synthetic DLL'
            'lib/netstandard2.1/JetDatabaseWriter.xml' = 'Synthetic documentation'
        }
        foreach ($file in 'README.md', 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'THIRD-PARTY-NOTICES.md') { $entries[$file] = [IO.File]::ReadAllText((Join-Path $repository $file)) }
        foreach ($name in $entries.Keys) {
            if ($name -eq $Missing) { continue }
            $writer = [IO.StreamWriter]::new($archive.CreateEntry($name).Open())
            try { $writer.Write($(if ($name -eq $Stale) { 'Incomplete license' } else { $entries[$name] })) } finally { $writer.Dispose() }
        }
    }
    finally { $archive.Dispose() }
    return $directory
}

function dotnet {
    $arguments = [string[]]$args
    $packageTestContext.Calls.Add($arguments)
    $stage = $arguments[0]
    Assert-Check ($stage -in 'restore', 'build', 'exec') "Unexpected dotnet operation: $stage"
    $global:LASTEXITCODE = if ($packageTestContext.Failure -eq $stage) { 17 } else { 0 }
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
Assert-Check ($packageTestContext.Calls.Count -eq 6) 'Both consumers must restore, build and execute.'
Assert-Check (($packageTestContext.Calls | ForEach-Object { $_[0] }) -join ',' -eq 'restore,build,exec,restore,build,exec') 'Consumer validation changed its operation sequence.'
foreach ($target in 'net10.0', 'net8.0') {
    $project = [xml][IO.File]::ReadAllText((Join-Path $pass.Output "$target/PackageConsumer.csproj"))
    Assert-Check ($project.Project.PropertyGroup.TargetFramework -eq $target) 'The consumer framework was lost.'
    Assert-Check ($project.Project.ItemGroup.PackageReference.Version -eq '[4.0.0-alpha.1]') 'The consumer must restore the exact artifact version.'
    Assert-Check ($project.SelectNodes('//ProjectReference').Count -eq 0) 'The package consumer must not compile a repository project.'
}
$config = [xml][IO.File]::ReadAllText((Join-Path $pass.Output 'NuGet.Config'))
Assert-Check ($config.SelectSingleNode('/configuration/packageSourceMapping/packageSource[@key="artifact"]/package').pattern -ceq 'JetDatabaseWriter') 'The shipping package must come from the isolated artifact source.'
foreach ($name in 'Directory.Build.props', 'Directory.Build.targets', 'Directory.Packages.props') { Assert-Check ([IO.File]::ReadAllText((Join-Path $pass.Output $name)) -eq '<Project />') 'The consumer inherited enclosing repository build configuration.' }
Assert-Check ([IO.File]::ReadAllText((Join-Path $pass.Output '.editorconfig')) -eq 'root = true') 'The consumer inherited repository analyzer configuration.'
Assert-Check (@($packageTestContext.Calls | Where-Object { $_[0] -eq 'restore' -and $_ -contains '--no-http-cache' -and $_ -contains '--packages' }).Count -eq 2) 'Both consumers require isolated restores.'
Assert-Check ($packageTestContext.Calls[2][-1] -eq '.NETCoreApp,Version=v10.0' -and $packageTestContext.Calls[5][-1] -eq '.NETStandard,Version=v2.1') 'Each consumer must verify its loaded library asset.'

foreach ($missing in 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'lib/netstandard2.1/JetDatabaseWriter.dll', 'lib/net10.0/JetDatabaseWriter.xml') {
    $name = 'missing-' + [IO.Path]::GetFileName($missing)
    $scenario = Invoke-Scenario $name (New-Package $name $missing)
    Assert-Check ($scenario.Error.Contains('missing ' + $missing.Split('/')[-1]) -or $scenario.Error.Contains('missing the ')) "Missing $missing was accepted: $($scenario.Error)"
    Assert-Check ($packageTestContext.Calls.Count -eq 0) 'A malformed archive reached the consumer commands.'
}
$stale = Invoke-Scenario 'stale' (New-Package 'stale' '' 'LICENSE-APACHE-2.0.txt')
Assert-Check ($stale.Error.Contains('stale or incomplete LICENSE-APACHE-2.0.txt')) 'An incomplete Apache license was accepted.'
foreach ($stage in 'restore', 'build', 'exec') {
    $scenario = Invoke-Scenario ('failed-' + $stage) $valid $stage
    Assert-Check ($scenario.Error.Contains('failed with exit code 17')) "$stage failure was not propagated."
    Assert-Check ($packageTestContext.Calls[-1][0] -eq $stage) "$stage failure did not stop subsequent work."
}
Write-Host "All package coordinator and malformed archive checks passed. Artifacts: $testRoot"
