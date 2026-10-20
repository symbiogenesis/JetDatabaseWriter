# Exercise the InferSharp report gate with synthetic reports, without Docker or network access.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$helper = Join-Path $PSScriptRoot '../assert-infersharp.ps1'
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('jdw-infersharp-tests-' + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($fixture) | Out-Null
$reportPath = Join-Path $fixture 'report.sarif'
$logPath = Join-Path $fixture 'analysis.log'
$coverage = "Method successfully translated: 10 (91%)`nMethod partially translated: 1 (9%)`nInstructions translated: 90 (90%)`nInstructions skipped: 10 (10%)`n"
[IO.File]::WriteAllText($logPath, $coverage)

function Assert-Scenario([string] $Name, [string] $Json, [string] $ExpectedError = '') {
    [IO.File]::WriteAllText($reportPath, $Json)
    $caught = $null
    $output = ''
    try { $output = (& $helper -ReportPath $reportPath -TranslationLogPath $logPath | Out-String) }
    catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or -not $caught.Contains($ExpectedError)) { throw "$Name expected '$ExpectedError', got '$caught'." }
    } elseif ($caught) {
        throw "$Name failed: $caught"
    } elseif (-not $output.Contains('InferSharp: 0 findings.')) {
        throw "$Name did not report the finding count."
    }
    "PASS: $Name"
}

try {
    $clean = '{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"Infer"}},"results":[]}]}'
    Assert-Scenario 'clean report succeeds' $clean
    Assert-Scenario 'single finding fails' ($clean.Replace('"results":[]', '"results":[{"ruleId":"NULLPTR_DEREFERENCE","message":{"text":"Null dereference"}}]')) 'InferSharp reported 1 finding(s)'
    Assert-Scenario 'warning finding also fails' ($clean.Replace('"results":[]', '"results":[{"level":"warning"}]')) 'InferSharp reported 1 finding(s)'
    Assert-Scenario 'suppressed finding is still reviewed' ($clean.Replace('"results":[]', '"results":[{"suppressions":[{"kind":"external"}]}]')) 'InferSharp reported 1 finding(s)'
    Assert-Scenario 'malformed JSON fails' '{' 'Invalid InferSharp SARIF'
    Assert-Scenario 'empty document fails' '' 'Invalid InferSharp SARIF'
    Assert-Scenario 'wrong SARIF version fails' ($clean.Replace('2.1.0', '1.0.0')) 'Invalid InferSharp SARIF'
    Assert-Scenario 'missing runs fails' '{"version":"2.1.0"}' 'Invalid InferSharp SARIF'
    Assert-Scenario 'empty runs fails' '{"version":"2.1.0","runs":[]}' 'Invalid InferSharp SARIF'
    Assert-Scenario 'missing results fails' ($clean.Replace(',"results":[]', '')) 'Invalid InferSharp SARIF'
    Assert-Scenario 'null results fails' ($clean.Replace('"results":[]', '"results":null')) 'Invalid InferSharp SARIF'
    Assert-Scenario 'object results fails' ($clean.Replace('"results":[]', '"results":{}')) 'Invalid InferSharp SARIF'
    Assert-Scenario 'wrong tool fails' ($clean.Replace('"Infer"', '"Other"')) 'Invalid InferSharp SARIF'
    Assert-Scenario 'later run findings fail' '{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"Infer"}},"results":[]},{"tool":{"driver":{"name":"Infer"}},"results":[{}]}]}' 'InferSharp reported 1 finding(s)'
    $caught = $null
    try { & $helper -ReportPath (Join-Path $fixture 'missing.sarif') -TranslationLogPath $logPath } catch { $caught = $_.Exception.Message }
    if (-not $caught -or -not $caught.Contains('Missing InferSharp SARIF')) { throw "Missing report was not rejected: $caught" }
    'PASS: missing report fails'
    [IO.File]::WriteAllText($logPath, $coverage.Replace('translated: 10', 'translated: 0'))
    Assert-Scenario 'zero translated methods fails' $clean 'Empty InferSharp translation'
    [IO.File]::WriteAllText($logPath, $coverage.Replace('translated: 90', 'translated: 0'))
    Assert-Scenario 'zero translated instructions fails' $clean 'Empty InferSharp translation'
    [IO.File]::WriteAllText($logPath, 'No coverage')
    Assert-Scenario 'missing statistics fails' $clean 'Invalid InferSharp coverage'
    [IO.File]::WriteAllText($logPath, $coverage + $coverage)
    Assert-Scenario 'ambiguous statistics fails' $clean 'Invalid InferSharp coverage'
    Remove-Item -LiteralPath $logPath
    Assert-Scenario 'missing translation log fails' $clean 'Missing InferSharp translation log'
}
finally {
    # Only this test owns the unique fixture directory.
    Remove-Item -LiteralPath $fixture -Recurse -Force
}
