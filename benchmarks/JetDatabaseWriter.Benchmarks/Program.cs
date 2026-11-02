using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

// Generated consumers repeat strict project analysis; hosted builds can exceed the default two minutes.
ManualConfig config = ManualConfig.Create(DefaultConfig.Instance).WithBuildTimeout(TimeSpan.FromMinutes(10));
IEnumerable<Summary> summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
return summaries.Any(summary => summary.HasCriticalValidationErrors || summary.Reports.Any(report => !report.Success || report.ResultStatistics is null)) ? 1 : 0;
