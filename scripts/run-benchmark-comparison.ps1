<#
.SYNOPSIS
Runs paired benchmarks, optionally reversing order and measuring an identical-revision control.
.DESCRIPTION
Used by benchmarks.yml on one hosted runner. Controlled order is baseline, head,
head, baseline, head, head. Every invocation uses fresh BenchmarkDotNet processes
and retains its own artifacts. Choose a fresh Results directory for every invocation.
Execute benchmarks only on CI, including short and dry jobs.
SummaryOnly reports partial results after failures and may run locally without executing benchmarks.
#>
param(
    [Parameter(Mandatory)] [string] $Results,
    [string] $Head,
    [string] $Baseline,
    [string] $Filter,
    [ValidateSet('default', 'short', 'medium', 'dry')] [string] $Job = 'default',
    [ValidateSet('paired', 'controlled')] [string] $Mode = 'paired',
    [switch] $SummaryOnly
)

$ErrorActionPreference = 'Stop'
# Anchor paths before changing checkout directories or calling .NET file APIs.
$Results = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Results)
$manifestPath = Join-Path $Results 'measurement.json'
if ($SummaryOnly) {
    if (-not (Test-Path -LiteralPath $manifestPath)) {
        'No benchmark run manifest was produced.'
        exit 0
    }
    $manifest = ConvertFrom-Json -InputObject ([IO.File]::ReadAllText($manifestPath)) -AsHashtable
    '# Hosted benchmark measurements'
    ''
    'Head: `' + $manifest.Head + '`. Baseline: `' + $manifest.Baseline + '`.'
    'Execution order: ' + ($manifest.Runs -join ', ') + '. All runs use the same host.'
    ''
    if (-not $manifest.Baseline) {
        Get-ChildItem -LiteralPath (Join-Path $Results 'head/results') -File -Filter '*-report-github.md' -ErrorAction SilentlyContinue | ForEach-Object { [IO.File]::ReadAllText($_.FullName) }
        exit 0
    }
    '## Baseline first'
    & "$PSScriptRoot/compare-benchmarks.ps1" -Baseline (Join-Path $Results 'baseline') -Head (Join-Path $Results 'head')
    if ($manifest.Mode -eq 'controlled') {
        ''
        '## Head first (repeat with reversed order)'
        & "$PSScriptRoot/compare-benchmarks.ps1" -Baseline (Join-Path $Results 'baseline-reverse') -Head (Join-Path $Results 'head-reverse')
        ''
        '## Identical head revision control (second / first)'
        & "$PSScriptRoot/compare-benchmarks.ps1" -Baseline (Join-Path $Results 'control-first') -Head (Join-Path $Results 'control-second')
    }
    exit 0
}

if ([string]::IsNullOrWhiteSpace($Head) -or [string]::IsNullOrWhiteSpace($Filter)) { throw 'Head and Filter are required to run benchmarks.' }
if ($Mode -eq 'controlled' -and -not $Baseline) { throw 'Controlled measurements require a baseline.' }
$filters = @($Filter -split '\s+' | Where-Object { $_ })
$jobArgs = if ($Job -ne 'default') { @('--job', $Job) } else { @() }
$runs = [ordered]@{}
if ($Baseline) { $runs['baseline'] = $Baseline }
$runs['head'] = $Head
if ($Mode -eq 'controlled') {
    $runs['head-reverse'] = $Head
    $runs['baseline-reverse'] = $Baseline
    $runs['control-first'] = $Head
    $runs['control-second'] = $Head
}
# Refuse stale artifacts before spending time measuring or replacing their manifest.
foreach ($run in $runs.GetEnumerator()) {
    if (-not (Test-Path -LiteralPath $run.Value -PathType Container)) { throw "Missing benchmark checkout: $($run.Value)." }
    $outputPath = Join-Path $Results $run.Key
    if (Test-Path -LiteralPath $outputPath) { throw "Benchmark artifacts already exist at '$outputPath'. Choose a fresh Results directory." }
}
[IO.Directory]::CreateDirectory($Results) | Out-Null
$headSha = git -C $Head rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve the head revision.' }
$baselineSha = if ($Baseline) {
    git -C $Baseline rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve the baseline revision.' }
} else { '' }
$manifest = [ordered]@{ Head = $headSha; Baseline = $baselineSha; Mode = $Mode; Job = $Job; Filter = $Filter; Runs = @($runs.Keys) }
[IO.File]::WriteAllText($manifestPath, ($manifest | ConvertTo-Json))
foreach ($run in $runs.GetEnumerator()) {
    $outputPath = Join-Path $Results $run.Key
    Push-Location -LiteralPath $run.Value
    try {
        dotnet run --project JetDatabaseWriter.Benchmarks -c Release --no-build -- --filter @filters @jobArgs --exporters json github --artifacts $outputPath
        if ($LASTEXITCODE -ne 0) { throw "The $($run.Key) benchmark process failed." }
    }
    finally { Pop-Location }
    $reports = @(Get-ChildItem -LiteralPath $outputPath -Recurse -File -Filter '*-report*.json')
    if ($reports.Count -eq 0) { throw "No benchmark results for $($run.Key)." }
    $caseCount = 0
    foreach ($reportPath in $reports) {
        $report = ConvertFrom-Json -InputObject ([IO.File]::ReadAllText($reportPath.FullName)) -Depth 64 -AsHashtable
        foreach ($case in $report.Benchmarks) {
            $caseCount++
            if ($null -eq $case.Statistics.Mean -or -not [double]::IsFinite([double]$case.Statistics.Mean) -or $case.Statistics.Mean -le 0) {
                throw "Invalid or missing mean for $($case.FullName) in $($run.Key)."
            }
            if ($null -ne $case.Memory -and ($null -eq $case.Memory.BytesAllocatedPerOperation -or -not [double]::IsFinite([double]$case.Memory.BytesAllocatedPerOperation) -or $case.Memory.BytesAllocatedPerOperation -lt 0)) {
                throw "Invalid or missing allocation measurement for $($case.FullName) in $($run.Key)."
            }
        }
    }
    if ($caseCount -eq 0) { throw "Empty benchmark reports for $($run.Key)." }
}
