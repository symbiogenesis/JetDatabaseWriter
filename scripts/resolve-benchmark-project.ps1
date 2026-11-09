# Resolve the project in each revision independently so published baselines can use the earlier layout.
[CmdletBinding()]
param([Parameter(Mandatory)][string] $Repository)

$ErrorActionPreference = 'Stop'
$net11Program = @'
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Toolchains.DotNetCli;

ConfigureNet11JobsIfNeeded();

// Generated consumers repeat strict project analysis; hosted builds can exceed the default two minutes.
ManualConfig config = ManualConfig.Create(DefaultConfig.Instance).WithBuildTimeout(TimeSpan.FromMinutes(10));
IEnumerable<Summary> summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
return summaries.Any(summary => summary.HasCriticalValidationErrors || summary.Reports.Any(report => !report.Success || report.ResultStatistics is null)) ? 1 : 0;

static void ConfigureNet11JobsIfNeeded()
{
    if (Environment.Version.Major < 11)
    {
        return;
    }

    string tfm = $"net{Environment.Version.Major}.0";
    string displayName = $".NET {Environment.Version.Major}.0";
    var runtime = CoreRuntime.CreateForNewVersion(tfm, displayName);
    FieldInfo? monikerField = typeof(Runtime).GetField(
        "<RuntimeMoniker>k__BackingField",
        BindingFlags.Instance | BindingFlags.NonPublic);
    monikerField?.SetValue(runtime, CoreRuntime.Core10_0.RuntimeMoniker);

    IToolchain toolchain = CsProjCoreToolchain.From(new NetCoreAppSettings(tfm, runtimeFrameworkVersion: null, name: displayName));

    Type? configParserType = typeof(BenchmarkSwitcher).Assembly.GetType("BenchmarkDotNet.ConsoleArguments.ConfigParser");
    FieldInfo? availableJobsField = configParserType?.GetField(
        "AvailableJobs",
        BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
    if (availableJobsField?.GetValue(null) is IDictionary<string, Job> availableJobs)
    {
        foreach (string key in availableJobs.Keys.ToArray())
        {
            Job existing = availableJobs[key];
            availableJobs[key] = existing
                .WithRuntime(runtime)
                .WithToolchain(toolchain)
                .WithId(existing.Id)
                .Freeze();
        }
    }
}
'@

foreach ($project in 'benchmarks/JetDatabaseWriter.Benchmarks/JetDatabaseWriter.Benchmarks.csproj', 'JetDatabaseWriter.Benchmarks/JetDatabaseWriter.Benchmarks.csproj') {
    $projectPath = Join-Path $Repository $project
    if (Test-Path -LiteralPath $projectPath -PathType Leaf) {
        $projectDir = Split-Path $projectPath -Parent
        $programPath = Join-Path $projectDir 'Program.cs'
        if ((Test-Path -LiteralPath $programPath -PathType Leaf) -and
            ([IO.File]::ReadAllText($projectPath) -match '<TargetFramework>\s*net11\.0\s*</TargetFramework>') -and
            ([IO.File]::ReadAllText($programPath) -notmatch 'ConfigureNet11JobsIfNeeded')) {
            [IO.File]::WriteAllText($programPath, $net11Program + [Environment]::NewLine)
            if (Test-Path -LiteralPath (Join-Path $projectDir 'bin/Release/net11.0/JetDatabaseWriter.Benchmarks.dll') -PathType Leaf) {
                $buildOutput = & dotnet build $projectPath -c Release --no-restore 2>&1
                Write-Host ($buildOutput -join [Environment]::NewLine)
                if ($LASTEXITCODE -ne 0) {
                    throw "Failed to rebuild '$projectPath' after configuring .NET 11 BenchmarkDotNet jobs."
                }
            }
        }
        return $project
    }
}
throw "Missing benchmark project in '$Repository'."
