# Infer exits successfully when it finds bugs; reject unreviewed findings and invalid analysis.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $ReportPath,
    [Parameter(Mandatory)][string] $TranslationLogPath,
    [string] $BaselinePath,
    [string] $TargetFramework,
    [string] $SourceRoot = (Split-Path $PSScriptRoot -Parent)
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

$findings = [Collections.Generic.List[object]]::new()
foreach ($run in $report.runs) {
    if ($run -isnot [Collections.IDictionary] -or $run.tool.driver.name -ne 'Infer' -or $run.results -isnot [array]) {
        throw 'Invalid InferSharp SARIF: each Infer run must contain a results array.'
    }
    foreach ($result in $run.results) { $findings.Add($result) }
}

function Get-Identity($Rule, $Path, $Line, $Fingerprint) { "$Rule|$Path|$Line|$Fingerprint" }

$reviewed = [Collections.Generic.Dictionary[string, bool]]::new([StringComparer]::Ordinal)
if ($BaselinePath) {
    try { $baseline = [IO.File]::ReadAllText($BaselinePath) | ConvertFrom-Json -AsHashtable -ErrorAction Stop }
    catch { throw "Invalid InferSharp baseline: $($_.Exception.Message)" }
    if ($baseline.version -ne 1 -or $baseline.analyzer -ne 'InferSharp v1.5' -or
        $baseline.reviewedRevision -notmatch '^[0-9a-f]{40}$' -or
        $baseline.sources -isnot [Collections.IDictionary] -or $baseline.sources.Count -eq 0 -or
        -not $TargetFramework -or $baseline.targets[$TargetFramework] -isnot [array]) {
        throw 'Invalid InferSharp baseline: expected a reviewed v1.5 baseline for this framework.'
    }
    foreach ($entry in $baseline.sources.GetEnumerator()) {
        if ($entry.Key -notmatch '^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$' -or '..' -in $entry.Key.Split('/') -or
            $entry.Value -notmatch '^[0-9a-f]{64}$') { throw 'Invalid InferSharp baseline: invalid source identity.' }
        $source = Join-Path $SourceRoot $entry.Key
        if (-not [IO.File]::Exists($source)) { throw "InferSharp reviewed source changed or is missing: $($entry.Key)" }
        $normalized = [IO.File]::ReadAllText($source).Replace("`r`n", "`n")
        $hash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($normalized))).ToLowerInvariant()
        if ($hash -ne $entry.Value) { throw "InferSharp reviewed source changed: $($entry.Key). Re-review the affected exceptions." }
    }
    foreach ($entry in $baseline.targets[$TargetFramework]) {
        if (-not $entry.ruleId -or -not $baseline.sources.Contains($entry.path) -or $entry.line -le 0 -or
            $entry.fingerprint -notmatch '^[0-9a-f]{1,32}$' -or [string]::IsNullOrWhiteSpace($entry.reason)) {
            throw 'Invalid InferSharp baseline: every exception needs an exact identity, reviewed source and reason.'
        }
        $identity = Get-Identity $entry.ruleId $entry.path $entry.line $entry.fingerprint
        if ($reviewed.ContainsKey($identity)) { throw 'Invalid InferSharp baseline: duplicate exception.' }
        $reviewed[$identity] = $false
    }
} elseif ($TargetFramework) { throw 'Invalid InferSharp baseline: TargetFramework requires BaselinePath.' }

$unreviewed = 0
foreach ($finding in $findings) {
    $location = if ($finding.locations -is [array] -and $finding.locations.Count -gt 0) { $finding.locations[0].physicalLocation } else { $null }
    $path = $location.artifactLocation.uri -replace '^file:/_/', ''
    $fingerprint = if ($finding.fingerprints -is [Collections.IDictionary]) { $finding.fingerprints['hash/v1'] } else { '' }
    $identity = Get-Identity $finding.ruleId $path $location.region.startLine $fingerprint
    if ($reviewed.ContainsKey($identity) -and -not $reviewed[$identity]) { $reviewed[$identity] = $true }
    else {
        $unreviewed++
        Write-Output "UNREVIEWED: $($finding.ruleId) $path $($location.region.startLine) $($finding.message.text)"
    }
}
Write-Output "InferSharp: $($findings.Count) findings; unreviewed findings: $unreviewed."
if ($unreviewed -gt 0) { throw "InferSharp reported $unreviewed unreviewed finding(s). Review the uploaded text and SARIF reports." }
if ($reviewed.Values -contains $false) { throw 'Stale InferSharp baseline: remove exceptions for findings no longer reported.' }
