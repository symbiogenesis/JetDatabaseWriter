# Inspect the actual shipping archive and exercise fresh NuGet consumers of both assets.
[CmdletBinding()]
param(
    [string] $PackageDirectory = 'artifacts/nupkg',
    [string] $OutputDirectory
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = Split-Path -Parent $PSScriptRoot
$packages = @(Get-ChildItem -LiteralPath $PackageDirectory -Filter 'JetDatabaseWriter.*.nupkg' -File)
if ($packages.Count -ne 1) { throw 'Expected exactly one JetDatabaseWriter NuGet package.' }
$package = $packages[0]
$archive = [IO.Compression.ZipFile]::OpenRead($package.FullName)
try {
    foreach ($name in 'README.md', 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'THIRD-PARTY-NOTICES.md') {
        $entry = $archive.GetEntry($name)
        if ($null -eq $entry) { throw "The shipping package is missing $name." }
        $reader = [IO.StreamReader]::new($entry.Open())
        try { $actual = $reader.ReadToEnd() } finally { $reader.Dispose() }
        $expected = [IO.File]::ReadAllText((Join-Path $repository $name))
        if ($actual -cne $expected) { throw "The shipping package has stale or incomplete $name." }
    }
    foreach ($target in 'net10.0', 'netstandard2.1') {
        if ($null -eq $archive.GetEntry("lib/$target/JetDatabaseWriter.dll")) { throw "The shipping package is missing the $target assembly." }
        if ($null -eq $archive.GetEntry("lib/$target/JetDatabaseWriter.xml")) { throw "The shipping package is missing the $target API documentation." }
    }
    $nuspecEntry = $archive.GetEntry('JetDatabaseWriter.nuspec')
    if ($null -eq $nuspecEntry) { throw 'The shipping package has no JetDatabaseWriter.nuspec.' }
    $reader = [IO.StreamReader]::new($nuspecEntry.Open())
    try { $nuspec = [xml]$reader.ReadToEnd() } finally { $reader.Dispose() }
    $version = [string]$nuspec.package.metadata.version
    if ($nuspec.package.metadata.id -cne 'JetDatabaseWriter' -or $version -notmatch '^[0-9A-Za-z.+-]+$') { throw 'Invalid shipping package identity.' }
}
finally { $archive.Dispose() }

if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-package-' + [Guid]::NewGuid().ToString('N'))
}
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if ((Test-Path -LiteralPath $OutputDirectory) -and @(Get-ChildItem -LiteralPath $OutputDirectory -Force).Count -ne 0) { throw 'Package consumers require a new or empty output directory.' }
[IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
$feed = Join-Path $OutputDirectory 'feed'
[IO.Directory]::CreateDirectory($feed) | Out-Null
Copy-Item -LiteralPath $package.FullName -Destination $feed
Copy-Item -LiteralPath (Join-Path $repository 'global.json') -Destination $OutputDirectory
# Keep caller-selected output paths independent of enclosing repository build settings.
foreach ($name in 'Directory.Build.props', 'Directory.Build.targets', 'Directory.Packages.props') {
    [IO.File]::WriteAllText((Join-Path $OutputDirectory $name), '<Project />')
}
[IO.File]::WriteAllText((Join-Path $OutputDirectory '.editorconfig'), 'root = true')
$config = Join-Path $OutputDirectory 'NuGet.Config'
$escapedFeed = [Security.SecurityElement]::Escape($feed)
[IO.File]::WriteAllText($config, @"
<configuration>
  <packageSources><clear /><add key="artifact" value="$escapedFeed" /><add key="nuget.org" value="https://api.nuget.org/v3/index.json" /></packageSources>
  <packageSourceMapping><clear /><packageSource key="artifact"><package pattern="JetDatabaseWriter" /></packageSource><packageSource key="nuget.org"><package pattern="*" /></packageSource></packageSourceMapping>
</configuration>
"@)
$source = @'
namespace PackageConsumer;

using System;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.Versioning;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        Assembly library = typeof(AccessReader).Assembly;
        string? target = library.GetCustomAttribute<TargetFrameworkAttribute>()?.FrameworkName;
        if (target != args[0]) { throw new InvalidDataException("Wrong NuGet asset loaded: " + target); }
        foreach (string name in new[] { "index_codes_genleg.txt.gz", "index_codes_ext_genleg.txt.gz", "index_codes_gen.txt.gz", "index_codes_ext_gen.txt.gz", "index_codes_gen_97.txt.gz", "index_mappings_ext_gen_97.txt.gz" })
        {
            using Stream resource = library.GetManifestResourceStream("JetDatabaseWriter.IndexCodeTables." + name) ?? throw new InvalidDataException("Missing collation table: " + name);
            using var gzip = new GZipStream(resource, CompressionMode.Decompress);
            using var text = new StreamReader(gzip);
            if (text.ReadToEnd().Length == 0) { throw new InvalidDataException("Empty collation table: " + name); }
        }
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            await using var stream = new MemoryStream();
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, leaveOpen: true))
            {
                await writer.CreateTableAsync("People", new[] { new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Name", typeof(string), maxLength: 50) }, new[] { new IndexDefinition("NameIndex", "Name") });
                await writer.InsertRowAsync("People", new RowValues { ["Id"] = 1, ["Name"] = "Andr\u00e9" });
            }
            stream.Position = 0;
            await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, leaveOpen: true))
            {
                int changed = await writer.UpdateRowsAsync("People", RowCriteria.Where("Id", 1), new RowValues { ["Name"] = "Zo\u00eb" });
                if (changed != 1) { throw new InvalidDataException("Package update did not find the inserted row."); }
            }
            stream.Position = 0;
            await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true);
            var rows = await reader.ReadTableAsync<Person>("People");
            if (rows.Count != 1 || rows[0].Id != 1 || rows[0].Name != "Zo\u00eb") { throw new InvalidDataException("Package create/read/write failed for " + format); }
            int indexed = 0;
            await foreach (Person person in reader.FromIndex<Person>("People", "NameIndex").WhereEquals("Zo\u00eb").ToRowsAsync())
            {
                if (person.Id != 1) { throw new InvalidDataException("Package text index returned the wrong row."); }
                indexed++;
            }
            if (indexed != 1) { throw new InvalidDataException("Package text index did not find the updated row."); }
        }
        Console.WriteLine("Package consumer passed: " + target);
    }
}

public sealed class Person
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
'@

function Invoke-PackageDotNet([string[]] $Arguments) {
    & dotnet @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Package consumer command failed with exit code ${LASTEXITCODE}: dotnet $($Arguments -join ' ')" }
}

Push-Location $OutputDirectory
try {
    # .NET 8 resolves the netstandard2.1 asset, then runs on the installed .NET 10 runtime.
    # An empty shared package cache and exact source mapping force use of this archive.
    foreach ($target in 'net10.0', 'net8.0') {
        $consumer = Join-Path $OutputDirectory $target
        [IO.Directory]::CreateDirectory($consumer) | Out-Null
        $project = Join-Path $consumer 'PackageConsumer.csproj'
        [IO.File]::WriteAllText($project, @"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><OutputType>Exe</OutputType><TargetFramework>$target</TargetFramework><Nullable>enable</Nullable><TreatWarningsAsErrors>true</TreatWarningsAsErrors><CheckForOverflowUnderflow>true</CheckForOverflowUnderflow><NuGetAuditMode>all</NuGetAuditMode></PropertyGroup>
  <ItemGroup><PackageReference Include="JetDatabaseWriter" Version="[$version]" /></ItemGroup>
</Project>
"@)
        [IO.File]::WriteAllText((Join-Path $consumer 'Program.cs'), $source)
        Invoke-PackageDotNet @('restore', $project, '--configfile', $config, '--packages', (Join-Path $OutputDirectory 'packages'), '--no-http-cache')
        Invoke-PackageDotNet @('build', $project, '-c', 'Release', '--no-restore')
        $expectedTarget = if ($target -eq 'net10.0') { '.NETCoreApp,Version=v10.0' } else { '.NETStandard,Version=v2.1' }
        Invoke-PackageDotNet @('exec', '--roll-forward', 'Major', (Join-Path $consumer "bin/Release/$target/PackageConsumer.dll"), $expectedTarget)
    }
}
finally { Pop-Location }
Write-Host "Shipping package and both consumer assets passed. Outputs: $OutputDirectory"
