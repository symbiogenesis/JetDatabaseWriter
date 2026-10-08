<#
.SYNOPSIS
Checks benchmark reporting and process orchestration with synthetic JSON and mocked git/dotnet commands.
.DESCRIPTION
Run with pwsh -NoProfile -File scripts/tests/benchmarks.tests.ps1. No real benchmarks or builds run.
Diagnostic fixtures and results are retained under artifacts/benchmark-script-checks.
#>
param([string] $Repository = (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent))
$ErrorActionPreference = 'Stop'
$checks = [Collections.Generic.List[object]]::new()
$root = Join-Path $Repository ('artifacts/benchmark-script-checks/' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($root) | Out-Null
$compare = Join-Path $Repository 'scripts/compare-benchmarks.ps1'
$runner = Join-Path $Repository 'scripts/run-benchmark-comparison.ps1'
function Check([string] $name, [scriptblock] $action) {
    try { & $action; $checks.Add([pscustomobject]@{ Check = $name; Result = 'PASS'; Detail = '' }) }
    catch { $checks.Add([pscustomobject]@{ Check = $name; Result = 'FAIL'; Detail = $_.Exception.Message }) }
}
function Assert([bool] $condition, [string] $message) { if (-not $condition) { throw $message } }
function Write-Report([string] $directory, $cases) {
    [IO.Directory]::CreateDirectory($directory) | Out-Null
    [IO.File]::WriteAllText((Join-Path $directory 'mock-report.json'), (@{ Benchmarks = @($cases) } | ConvertTo-Json -Depth 20))
}
function New-Case([string] $name = 'Sample.Read', $mean = 100, $allocated = 0) {
    @{ FullName = $name; Type = 'Sample'; Method = 'Read'; Parameters = ''; Statistics = @{ Mean = $mean; ConfidenceInterval = @{ Margin = 1 } }; Memory = @{ BytesAllocatedPerOperation = $allocated } }
}
$base = Join-Path $root 'compare-base'
$headPath = Join-Path $root 'compare-head'
Check 'Case-sensitive benchmark identities' {
    Write-Report $base @((New-Case 'Sample.Read(value: a)' 100), (New-Case 'Sample.Read(value: A)' 200))
    Write-Report $headPath @((New-Case 'Sample.Read(value: a)' 50), (New-Case 'Sample.Read(value: A)' 100))
    $report = & $compare -Baseline $base -Head $headPath
    Assert (@($report -split "`n" | Where-Object { $_ -match '^\| Sample\.' }).Count -eq 2) 'Distinct case-sensitive parameter values collapsed.'
}
Check 'Missing metrics remain unavailable' {
    $missing = New-Case
    $missing.Statistics.Remove('Mean')
    $missing.Statistics.ConfidenceInterval.Remove('Margin')
    $missing.Memory.Remove('BytesAllocatedPerOperation')
    Write-Report $base @($missing)
    Write-Report $headPath @($missing)
    $report = & $compare -Baseline $base -Head $headPath
    Assert ($report.Contains('| Sample.Read | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |')) 'Missing measurements were rendered as real zero measurements.'
}
Check 'Ratios, error margins and zero allocations' {
    Write-Report $base @((New-Case 'Sample.Read' 100))
    Write-Report $headPath @((New-Case 'Sample.Read' 50))
    $report = & $compare -Baseline $base -Head $headPath
    Assert ($report.Contains('| Sample.Read | 100.0 ns | 1.0 ns | 50.0 ns | 1.0 ns | 0.50 | 0 B | 0 B | 1.00 |')) 'Valid comparison changed.'
}
$checkout = Join-Path $root 'head checkout'
$baselineCheckout = Join-Path $root 'baseline checkout'
[IO.Directory]::CreateDirectory($checkout) | Out-Null
[IO.Directory]::CreateDirectory($baselineCheckout) | Out-Null
$global:BenchmarkCheckInvocations = [Collections.Generic.List[object]]::new()
$global:BenchmarkCheckMean = 100
$global:BenchmarkCheckAllocation = 0
$global:BenchmarkCheckProduceReport = $true
function git {
    $global:LASTEXITCODE = 0
    '0123456789012345678901234567890123456789'
}
function dotnet {
    $arguments = @($args)
    $output = $arguments[[Array]::IndexOf($arguments, '--artifacts') + 1]
    $global:BenchmarkCheckInvocations.Add([pscustomobject]@{ Checkout = $PWD.Path; Output = $output; Arguments = $arguments })
    if ($global:BenchmarkCheckProduceReport) {
        $resolved = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($output)
        Write-Report (Join-Path $resolved 'results') @((New-Case 'Sample.Read' $global:BenchmarkCheckMean $global:BenchmarkCheckAllocation))
    }
    $global:LASTEXITCODE = 0
}
Check 'Relative output stays anchored to invocation directory' {
    Push-Location -LiteralPath $root
    try {
        & $runner -Head $checkout -Results 'relative-results' -Filter '*Read*'
        Assert ([IO.Path]::IsPathRooted($global:BenchmarkCheckInvocations[-1].Output)) 'Benchmark process received a relative artifacts directory.'
        Assert (Test-Path -LiteralPath (Join-Path $root 'relative-results/head/results/mock-report.json')) 'Report escaped the requested results root.'
    }
    finally { Pop-Location }
}
Check 'Controlled measurement order and arguments' {
    $global:BenchmarkCheckInvocations.Clear()
    & $runner -Head $checkout -Baseline $baselineCheckout -Results (Join-Path $root 'controlled') -Filter '*Read* *Write*' -Mode controlled -Job dry
    Assert (($global:BenchmarkCheckInvocations | ForEach-Object { Split-Path $_.Output -Leaf }) -join ',' -eq 'baseline,head,head-reverse,baseline-reverse,control-first,control-second') 'Measurement order changed.'
    Assert ($global:BenchmarkCheckInvocations.Count -eq 6) 'Incorrect process count.'
    foreach ($call in $global:BenchmarkCheckInvocations) {
        Assert (($call.Arguments -join ' ').Contains('--filter *Read* *Write* --job dry --exporters json github')) 'Filter, job or exporter arguments changed.'
    }
}
Check 'Whitespace filter fails before measurement' {
    $global:BenchmarkCheckInvocations.Clear()
    $rejected = $false
    try { & $runner -Head $checkout -Results (Join-Path $root 'blank-filter') -Filter '  ' } catch { $rejected = $true }
    Assert ($rejected -and $global:BenchmarkCheckInvocations.Count -eq 0) 'Empty filter launched a benchmark process.'
}
Check 'Stale reports cannot validate an empty run' {
    $results = Join-Path $root 'stale'
    Write-Report (Join-Path $results 'head/results') @((New-Case))
    $global:BenchmarkCheckProduceReport = $false
    $global:BenchmarkCheckInvocations.Clear()
    $rejected = $false
    try { & $runner -Head $checkout -Results $results -Filter '*Read*' } catch { $rejected = $true }
    finally { $global:BenchmarkCheckProduceReport = $true }
    Assert ($rejected -and $global:BenchmarkCheckInvocations.Count -eq 0) 'Stale output was reused or expensive measurement ran before rejecting it.'
}
Check 'Nonfinite mean is rejected' {
    $global:BenchmarkCheckMean = 'NaN'
    $rejected = $false
    try { & $runner -Head $checkout -Results (Join-Path $root 'nan-mean') -Filter '*Read*' } catch { $rejected = $_.Exception.Message -like 'Invalid or missing mean*' }
    finally { $global:BenchmarkCheckMean = 100 }
    Assert $rejected 'NaN mean passed measurement validation.'
}
Check 'Nonfinite allocation is rejected' {
    $global:BenchmarkCheckAllocation = 'Infinity'
    $rejected = $false
    try { & $runner -Head $checkout -Results (Join-Path $root 'infinite-allocation') -Filter '*Read*' } catch { $rejected = $_.Exception.Message -like 'Invalid or missing allocation*' }
    finally { $global:BenchmarkCheckAllocation = 0 }
    Assert $rejected 'Infinite allocation passed measurement validation.'
}
Check 'Partial controlled summaries retain all comparisons' {
    $partial = Join-Path $root 'partial'
    [IO.Directory]::CreateDirectory($partial) | Out-Null
    [IO.File]::Copy((Join-Path $root 'controlled/measurement.json'), (Join-Path $partial 'measurement.json'))
    $summary = (& $runner -Results $partial -SummaryOnly) -join "`n"
    Assert ($summary.Contains('## Baseline first') -and $summary.Contains('## Head first') -and $summary.Contains('## Identical head revision control')) 'Controlled summaries lost comparisons.'
}
Check 'Dry confidence intervals remain unavailable' {
    $dry = New-Case
    $dry.Statistics.ConfidenceInterval.Margin = 'NaN'
    Write-Report $base @($dry)
    Write-Report $headPath @($dry)
    $report = & $compare -Baseline $base -Head $headPath
    Assert ($report.Contains('| Sample.Read | 100.0 ns | n/a | 100.0 ns | n/a | 1.00 | 0 B | 0 B | 1.00 |')) 'Nonfinite dry-run errors were rendered as numeric measurements.'
}
Check 'Missing run manifests summarize without error' {
    $summary = & $runner -Results (Join-Path $root 'missing-manifest') -SummaryOnly
    Assert ($summary -eq 'No benchmark run manifest was produced.') 'Missing run manifest did not produce the partial-result diagnostic.'
}
Remove-Variable -Scope Global -Name BenchmarkCheckInvocations, BenchmarkCheckMean, BenchmarkCheckAllocation, BenchmarkCheckProduceReport
$checks | Format-Table -Wrap -AutoSize
[IO.File]::WriteAllText((Join-Path $root 'checks.json'), ($checks | ConvertTo-Json))
Write-Output "CHECK_ARTIFACTS=$root"
if (@($checks | Where-Object Result -eq 'FAIL').Count -gt 0) { exit 1 }