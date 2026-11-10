# EF query performance qualification

The [paired hosted benchmark run](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/42750120057)
compares `9282d92cdf2d220bacee0f4a95008d5327d4e14b` with the published
before-fix harness at `82183e882c714ec67f92f826a4328f6f86ee9bbc`.
Both revisions passed their exact-revision CI gates:
[candidate](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/42747855629)
and [baseline](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/42744865201).

`EfOrderedQueryBenchmarks` uses an ACE database in a caller-owned memory stream,
an integer primary key, a Boolean residual filter and scalar no-tracking
projections. The ordered first-row case stops an unbounded enumeration after one
row; the filtered-page case reads ten even keys. The unordered first-row control
qualifies the cost of diagnostics disabled by the default logging configuration.
These results cover warm queries on Windows x64 and .NET 11 RC3, using
BenchmarkDotNet 0.15.8's default adaptive job. Baseline and candidate ran
back to back on the same hosted runner.

Times are microseconds, shown as mean plus/minus BenchmarkDotNet Error
(half the 99.9% confidence interval). Allocations are bytes per operation.

| Query | Rows | Baseline time | Candidate time | Baseline allocation | Candidate allocation |
|---|---:|---:|---:|---:|---:|
| Ordered first row | 100 | 1388.894 +/- 13.579 | 19.088 +/- 0.317 | 47,340 | 15,512 |
| Ordered filtered page | 100 | 1544.663 +/- 16.195 | 35.393 +/- 0.278 | 47,851 | 22,120 |
| Unordered first row | 100 | 10.084 +/- 0.082 | 10.299 +/- 0.130 | 7,264 | 7,336 |
| Ordered first row | 10,000 | 9032.523 +/- 155.170 | 27.808 +/- 0.265 | 1,823,625 | 28,056 |
| Ordered filtered page | 10,000 | 1710.985 +/- 33.627 | 45.976 +/- 0.258 | 60,928 | 34,840 |
| Unordered first row | 10,000 | 10.931 +/- 0.312 | 10.401 +/- 0.061 | 7,264 | 7,368 |

The ordered-query improvements exceed both error columns. The 10,000-row
first-row case uses about 1.54% of the baseline allocation and 0.31% of its time.
The unordered control shows an additional 72 to 104 bytes per operation;
timing changes of +2.1% and -4.9% do not establish a consistent timing regression
on a hosted runner. Diagnostics are qualified as a small constant allocation
cost, without a zero-overhead timing claim.

All twelve measured cases have valid means, errors and allocations. The run's
`benchmark-results` artifact retains `measurement.json`, `summary.md` and the
baseline/head BenchmarkDotNet JSON and Markdown reports. These measurements do
not qualify cold file I/O, other platforms, other key domains or full navigation
graphs; their execution rules remain in [EF query execution](ef-query-execution.md).
