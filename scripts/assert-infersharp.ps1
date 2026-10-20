# Infer exits successfully when it finds bugs; require a valid report with no findings.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $ReportPath,
    [Parameter(Mandatory)][string] $TranslationLogPath
)

$ErrorActionPreference = 'Stop'
if (-not [IO.File]::Exists($ReportPath)) { throw "Missing InferSharp SARIF report: $ReportPath" }
try { $report = [IO.File]::ReadAllText($ReportPath) | ConvertFrom-Json -AsHashtable -ErrorAction Stop }
catch { throw "Invalid InferSharp SARIF: $($_.Exception.Message)" }
if ($report -isnot [Collections.IDictionary] -or $report.version -ne '2.1.0' -or
    $report.runs -isnot [array] -or $report.runs.Count -eq 0) {
    throw 'Invalid InferSharp SARIF: expected SARIF 2.1.0 with at least one run.'
}
# Cilsil can skip unsupported methods and still exit successfully. Retain its coverage
# statistics, and never accept an empty translation as a clean analysis.
if (-not [IO.File]::Exists($TranslationLogPath)) { throw "Missing InferSharp translation log: $TranslationLogPath" }
$log = [IO.File]::ReadAllText($TranslationLogPath)
foreach ($label in 'Method successfully translated', 'Method partially translated', 'Instructions translated', 'Instructions skipped') {
    $matches = [regex]::Matches($log, '(?m)^' + [regex]::Escape($label) + ': (?<count>[0-9]+) \((?<percent>[0-9]+)%\)\r?$')
    if ($matches.Count -ne 1) { throw "Invalid InferSharp coverage: missing or ambiguous '$label' statistic." }
    $statistic = $matches[0]
    if ($label -in 'Method successfully translated', 'Instructions translated' -and [long]$statistic.Groups['count'].Value -eq 0) {
        throw "Empty InferSharp translation: $label must be positive."
    }
    Write-Output $statistic.Value.TrimEnd()
}

$count = 0
foreach ($run in $report.runs) {
    if ($run -isnot [Collections.IDictionary] -or $run.tool.driver.name -ne 'Infer' -or $run.results -isnot [array]) {
        throw 'Invalid InferSharp SARIF: each Infer run must contain a results array.'
    }
    $count += $run.results.Count
}
Write-Output "InferSharp: $count findings."
if ($count -gt 0) { throw "InferSharp reported $count finding(s). Review the uploaded text and SARIF reports." }
