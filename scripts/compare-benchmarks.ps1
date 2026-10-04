<#
.SYNOPSIS
Compares two BenchmarkDotNet result sets and writes a markdown table to the output.

.DESCRIPTION
Reads every *-report*.json under each directory (a BenchmarkDotNet --artifacts directory or its results folder),
matches benchmarks by their full name (type, method and parameters), and prints, for each one, the mean time, its
error and the bytes allocated per operation in both sets, with the head/baseline ratio. A ratio below 1 is faster or
smaller. The error is BenchmarkDotNet's Error column: half the width of the mean's 99.9% confidence interval.
Benchmarks found in only one set are listed after the table. The benchmarks workflow
(.github/workflows/benchmarks.yml) calls this script to write its run summary.

.EXAMPLE
pwsh -NoProfile -File scripts/compare-benchmarks.ps1 -Baseline artifacts/bench-main -Head artifacts/bench-branch
#>
param(
    [Parameter(Mandatory)] [string] $Baseline,
    [Parameter(Mandatory)] [string] $Head
)

$ErrorActionPreference = 'Stop'

function Read-Results([string] $dir) {
    $map = [ordered]@{}
    if (-not (Test-Path -LiteralPath $dir)) { return $map }
    foreach ($file in Get-ChildItem -LiteralPath $dir -Recurse -File -Filter '*-report*.json') {
        $report = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json -Depth 64
        foreach ($b in $report.Benchmarks) {
            $label = if ($b.Type -and $b.Method) { "$($b.Type).$($b.Method)" + $(if ($b.Parameters) { "($($b.Parameters))" } else { '' }) }
                     else { $b.FullName -replace '^.*?\.(\w+\.\w+(\(.*\))?)$', '$1' }
            $map[$b.FullName] = [pscustomobject]@{
                Label = $label
                Mean = if ($b.Statistics) { [double]$b.Statistics.Mean } else { $null }
                # BenchmarkDotNet's Error column is the confidence interval's margin, not the standard error.
                Error = if ($b.Statistics.ConfidenceInterval) { [double]$b.Statistics.ConfidenceInterval.Margin } else { $null }
                Allocated = if ($b.Memory) { [double]$b.Memory.BytesAllocatedPerOperation } else { $null }
            }
        }
    }
    return $map
}

function Format-Time($ns) {
    if ($null -eq $ns) { return 'n/a' }
    if ($ns -ge 1e9) { return '{0:N2} s' -f ($ns / 1e9) }
    if ($ns -ge 1e6) { return '{0:N2} ms' -f ($ns / 1e6) }
    if ($ns -ge 1e3) { return '{0:N2} μs' -f ($ns / 1e3) }
    return '{0:N1} ns' -f $ns
}

function Format-Bytes($bytes) {
    if ($null -eq $bytes) { return 'n/a' }
    if ($bytes -ge 1MB) { return '{0:N2} MB' -f ($bytes / 1MB) }
    if ($bytes -ge 1KB) { return '{0:N2} KB' -f ($bytes / 1KB) }
    return '{0:N0} B' -f $bytes
}

function Format-Ratio($head, $base) {
    if ($null -eq $head -or $null -eq $base) { return 'n/a' }
    if ($base -eq 0) { return $(if ($head -eq 0) { '1.00' } else { 'n/a' }) }
    return '{0:N2}' -f ($head / $base)
}

$base = Read-Results $Baseline
$headResults = Read-Results $Head

$out = [System.Collections.Generic.List[string]]::new()
$out.Add('## Benchmark comparison')
$out.Add('')
$out.Add('The baseline and the head ran back to back on the same runner. Ratio is head / baseline: below 1.00 is faster or smaller. Error is BenchmarkDotNet''s Error column, half the width of the 99.9% confidence interval of the mean. On hosted runners, differences of a few percent are usually noise; compare a change against both error columns before reading anything into it.')
$out.Add('')
$out.Add('| Benchmark | Baseline mean | Baseline error | Head mean | Head error | Ratio | Baseline allocated | Head allocated | Ratio |')
$out.Add('|---|---:|---:|---:|---:|---:|---:|---:|---:|')
foreach ($name in $headResults.Keys) {
    if (-not $base.Contains($name)) { continue }
    $h = $headResults[$name]
    $b = $base[$name]
    $out.Add(('| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} |' -f
        ($h.Label -replace '\|', '\|'), (Format-Time $b.Mean), (Format-Time $b.Error), (Format-Time $h.Mean), (Format-Time $h.Error),
        (Format-Ratio $h.Mean $b.Mean), (Format-Bytes $b.Allocated), (Format-Bytes $h.Allocated), (Format-Ratio $h.Allocated $b.Allocated)))
}

$onlyHead = @($headResults.Keys | Where-Object { -not $base.Contains($_) })
$onlyBase = @($base.Keys | Where-Object { -not $headResults.Contains($_) })
if ($onlyHead.Count -gt 0) {
    $out.Add('')
    $out.Add('Only in the head: ' + (($onlyHead | ForEach-Object { '`' + $headResults[$_].Label + '`' }) -join ', '))
}
if ($onlyBase.Count -gt 0) {
    $out.Add('')
    $out.Add('Only in the baseline: ' + (($onlyBase | ForEach-Object { '`' + $base[$_].Label + '`' }) -join ', '))
}
if ($headResults.Count -eq 0 -and $base.Count -eq 0) { $out.Clear(); $out.Add('No benchmark results were found.') }

$out -join "`n"
