# Verify the shipping license inventory without building or accessing package feeds.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
foreach ($projectPath in 'src/JetDatabaseWriter/JetDatabaseWriter.csproj', 'src/JetDatabaseWriter.EntityFrameworkCore/JetDatabaseWriter.EntityFrameworkCore.csproj') {
    $project = [xml][IO.File]::ReadAllText((Join-Path $repository $projectPath))
    if ($project.Project.PropertyGroup.PackageProjectUrl -cne 'https://symbiogenesis.github.io/JetDatabaseWriter/') {
        throw "The NuGet package project URL in $projectPath must be https://symbiogenesis.github.io/JetDatabaseWriter/."
    }
    foreach ($name in 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'THIRD-PARTY-NOTICES.md') {
        $item = @($project.SelectNodes('/Project/ItemGroup/None') | Where-Object { $_.Include -eq ('..\..\' + $name) })
        if ($item.Count -ne 1 -or $item[0].Pack -ne 'true' -or $item[0].PackagePath -ne '\') {
            throw "The NuGet package in $projectPath must include the complete $name at its root."
        }
        if (-not (Test-Path -LiteralPath (Join-Path $repository $name) -PathType Leaf)) { throw "Missing shipping license: $name" }
    }
}
$project = [xml][IO.File]::ReadAllText((Join-Path $repository 'src/JetDatabaseWriter/JetDatabaseWriter.csproj'))
$apache = [IO.File]::ReadAllText((Join-Path $repository 'LICENSE-APACHE-2.0.txt'))
foreach ($section in 'TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION', '4. Redistribution.', '9. Accepting Warranty or Additional Liability.', 'END OF TERMS AND CONDITIONS') {
    if (-not $apache.Contains($section)) { throw "The Apache license is incomplete: missing $section" }
}
$notices = [IO.File]::ReadAllText((Join-Path $repository 'THIRD-PARTY-NOTICES.md'))
foreach ($resource in $project.SelectNodes('/Project/ItemGroup/EmbeddedResource')) {
    $name = [IO.Path]::GetFileName($resource.Include.Replace('\', '/'))
    if (-not $notices.Contains($name)) { throw "Missing attribution for the shipped resource $name." }
}
Write-Host 'Shipping license declarations and resource attribution passed.'

# Command doubles verify fresh consumer isolation without restoring packages or compiling.
$testRoot = Join-Path ([IO.Path]::GetTempPath()) ('jdw-package-consumers-' + [Guid]::NewGuid().ToString('N'))
$packageDirectory = Join-Path $testRoot 'packages'
[IO.Directory]::CreateDirectory($packageDirectory) | Out-Null
foreach ($identity in 'JetDatabaseWriter', 'JetDatabaseWriter.EntityFrameworkCore') {
    $archive = [IO.Compression.ZipFile]::Open((Join-Path $packageDirectory "$identity.1.0.0.nupkg"), [IO.Compression.ZipArchiveMode]::Create)
    try {
        foreach ($name in 'README.md', 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'THIRD-PARTY-NOTICES.md') {
            $writer = [IO.StreamWriter]::new($archive.CreateEntry($name).Open())
            try { $writer.Write([IO.File]::ReadAllText((Join-Path $repository $name))) } finally { $writer.Dispose() }
        }
        $targets = if ($identity -eq 'JetDatabaseWriter') { 'net11.0', 'netstandard2.1' } else { 'net11.0', 'net10.0' }
        foreach ($target in $targets) {
            foreach ($extension in 'dll', 'xml') { [void]$archive.CreateEntry("lib/$target/$identity.$extension") }
        }
        $writer = [IO.StreamWriter]::new($archive.CreateEntry("$identity.nuspec").Open())
        try { $writer.Write("<package><metadata><id>$identity</id><version>1.0.0</version><projectUrl>https://symbiogenesis.github.io/JetDatabaseWriter/</projectUrl></metadata></package>") } finally { $writer.Dispose() }
    }
    finally { $archive.Dispose() }
}
$consumerCalls = [Collections.Generic.List[object]]::new()
$consumerFailure = $false
function dotnet {
    $arguments = [string[]]$args
    $global:LASTEXITCODE = 0
    if ($arguments[0] -eq '--list-runtimes') {
        'Microsoft.NETCore.App 10.0.9 [runtime]'
        'Microsoft.NETCore.App 11.0.0-rc.3.26522.148 [runtime]'
        return
    }
    $consumerCalls.Add($arguments)
    if ($consumerFailure -and $arguments[0] -eq 'exec' -and ($arguments -join '/') -like '*ef-net10.0*') { $global:LASTEXITCODE = 9 }
}
$consumerOutput = Join-Path $testRoot 'consumers'
& (Join-Path $PSScriptRoot '../test-package.ps1') -PackageDirectory $packageDirectory -OutputDirectory $consumerOutput
$restores = @($consumerCalls | Where-Object { $_[0] -eq 'restore' })
if ($restores.Count -ne 4) { throw 'Package validation must restore two core and two EF-only consumers.' }
$configuration = [xml][IO.File]::ReadAllText((Join-Path $consumerOutput 'NuGet.Config'))
$artifactMapping = @($configuration.configuration.packageSourceMapping.packageSource | Where-Object key -eq 'artifact')
foreach ($identity in 'JetDatabaseWriter', 'JetDatabaseWriter.EntityFrameworkCore') {
    if ($artifactMapping.package.pattern -cnotcontains $identity) { throw "The artifact feed must map $identity explicitly." }
}
foreach ($consumer in 'net11.0', 'net8.0', 'ef-net11.0', 'ef-net10.0') {
    $directory = Join-Path $consumerOutput $consumer
    $projectPath = Join-Path $directory 'PackageConsumer.csproj'
    $project = [xml][IO.File]::ReadAllText($projectPath)
    $references = @($project.SelectNodes('/Project/ItemGroup/PackageReference'))
    $expectedPackage = if ($consumer.StartsWith('ef-')) { 'JetDatabaseWriter.EntityFrameworkCore' } else { 'JetDatabaseWriter' }
    if ($references.Count -ne 1 -or $references[0].Include -cne $expectedPackage -or $references[0].Version -cne '[1.0.0]') { throw "Consumer $consumer must reference only the exact packed $expectedPackage." }
    $expectedFramework = $consumer.Replace('ef-', '')
    if ($project.Project.PropertyGroup.TargetFramework -cne $expectedFramework) { throw "Wrong consumer framework for $consumer." }
    $restore = @($restores | Where-Object { $_[1] -eq $projectPath })
    if ($restore.Count -ne 1 -or $restore[0] -notcontains (Join-Path $consumerOutput 'packages') -or $restore[0] -notcontains '--no-http-cache') { throw "Consumer $consumer must use the isolated package cache." }
    $execution = @($consumerCalls | Where-Object { $_[0] -eq 'exec' -and $_ -contains (Join-Path $directory "bin/Release/$expectedFramework/PackageConsumer.dll") })
    $runtimeMajor = if ($expectedFramework -eq 'net11.0') { '11' } else { '10' }
    $expectedCore = if ($runtimeMajor -eq '11') { '.NETCoreApp,Version=v11.0' } else { '.NETStandard,Version=v2.1' }
    if ($execution.Count -ne 1 -or $execution[0] -notcontains $expectedCore -or $execution[0] -notcontains $runtimeMajor -or $execution[0] -notcontains 'Disable') { throw "Consumer $consumer must verify its library asset and exact runtime." }
    if ($consumer.StartsWith('ef-') -and $execution[0] -notcontains ".NETCoreApp,Version=v$runtimeMajor.0") { throw "Consumer $consumer must verify the EF provider asset." }
}
$consumerFailure = $true
$caught = ''
try { & (Join-Path $PSScriptRoot '../test-package.ps1') -PackageDirectory $packageDirectory -OutputDirectory (Join-Path $testRoot 'failure') }
catch { $caught = $_.Exception.Message }
if (-not $caught.Contains('failed with exit code 9')) { throw 'A failing EF package consumer must fail package validation.' }
Write-Host "Shipping package consumer isolation and failure propagation passed. Outputs: $testRoot"
