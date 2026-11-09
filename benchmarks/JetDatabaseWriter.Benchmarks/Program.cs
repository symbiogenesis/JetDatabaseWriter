using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

// Exact-revision CI analyzes these projects; generated consumers only need to compile them.
ManualConfig config = ManualConfig.Create(DefaultConfig.Instance)
    .AddJob(Job.Default.WithMsBuildArguments("/p:RunAnalyzers=false").AsMutator())
    .WithBuildTimeout(TimeSpan.FromMinutes(10));
IEnumerable<Summary> summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
return summaries.Any(summary => summary.HasCriticalValidationErrors || summary.Reports.Any(report => !report.Success || report.ResultStatistics is null)) ? 1 : 0;
