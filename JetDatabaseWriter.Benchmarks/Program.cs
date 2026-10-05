using System.Linq;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

Summary[] summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return summaries.Any(summary => summary.HasCriticalValidationErrors
    || summary.Reports.Any(report => !report.Success || report.ResultStatistics is null)) ? 1 : 0;
