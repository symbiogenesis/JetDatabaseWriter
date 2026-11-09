# Check real xUnit outcomes rather than relying on process success or skip totals.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$helper = Join-Path $PSScriptRoot '../assert-test-results.ps1'
$root = Join-Path ([IO.Path]::GetTempPath()) "jdw-test-results-$([guid]::NewGuid().ToString('N'))"
[IO.Directory]::CreateDirectory($root) | Out-Null

function Assert-Result([string] $Name, [string] $Tests, [string] $ExpectedError = '', [switch] $RequireNoSkips, [string[]] $RequiredMethods = @(), [string] $Errors = '') {
    $directory = Join-Path $root $Name
    [IO.Directory]::CreateDirectory($directory) | Out-Null
    [IO.File]::WriteAllText((Join-Path $directory 'tests.xml'), "<assemblies><assembly><errors>$Errors</errors><collection>$Tests</collection></assembly></assemblies>")
    $caught = ''
    try { & $helper -ResultsDirectory $directory -RequireNoSkips:$RequireNoSkips -RequiredMethods $RequiredMethods } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught.Contains($ExpectedError)) { throw "$Name expected '$ExpectedError', received '$caught'." }
    } elseif ($caught) { throw "$Name failed: $caught" }
    $script:summary = Get-Content -LiteralPath (Join-Path $directory 'summary.json') -Raw | ConvertFrom-Json
    $script:markdown = Get-Content -LiteralPath (Join-Path $directory 'summary.md') -Raw
    $script:failure = $caught
    "PASS: $Name"
}

$pass = '<test name="Case" type="Example.Tests" method="Case" result="Pass" />'
$dao = '<test name="Dao" type="Example.Dao" method="Native" result="Skip"><reason><![CDATA[Requires Microsoft Access (DAO.DBEngine.120)]]></reason></test>'
$explicit = '<test name="Fuzz" type="JetDatabaseWriter.Tests.Fuzz.AccessReaderFuzzTests" method="FuzzAccessReader" result="NotRun" />'
Assert-Result 'ordinary' ($pass + $dao + $explicit)
if ($summary.counts.Pass -ne 1 -or $summary.guardedSkips -ne 1 -or $summary.explicitNotRun -ne 1) { throw 'Semantic outcome counts were lost.' }
$explicitSkip = $explicit.Replace('result="NotRun" />', 'result="Skip"><reason>Not run (due to explicit test filtering)</reason></test>')
Assert-Result 'explicit-skip-encoding' ($pass + $explicitSkip)
if ($summary.guardedSkips -ne 0 -or $summary.explicitNotRun -ne 1) { throw 'Explicit Skip outcomes were mislabeled as DAO guards.' }
Assert-Result 'native-all-pass' $pass -RequireNoSkips -RequiredMethods 'Example.Tests.Case'
Assert-Result 'native-skip-refused' ($pass + $dao) 'Unexpected skipped test' -RequireNoSkips
Assert-Result 'native-explicit-refused' ($pass + $explicit) 'Unexpected not-run test' -RequireNoSkips
Assert-Result 'skip-not-allowed' ($pass + '<test name="Regression" type="Example.Tests" method="Other" result="Skip"><reason>Not implemented yet</reason></test>') 'Unexpected skipped test'
Assert-Result 'spoofed-dao-prefix' ($pass + '<test name="Regression" result="Skip"><reason>Requires Microsoft Access (DAO.DBEngine.120): other reason</reason></test>') 'Unexpected skipped test'
Assert-Result 'spoofed-explicit-reason' ($pass + '<test name="Regression" type="Example.Tests" method="Other" result="Skip"><reason>Not run (due to explicit test filtering)</reason></test>') 'Unexpected skipped test'
Assert-Result 'new-explicit-test-refused' ($pass + '<test name="Regression" type="Example.Tests" method="Other" result="NotRun" />') 'Unexpected not-run test'
Assert-Result 'required-native-missing' $pass 'Required method did not pass' -RequiredMethods 'Example.Tests.Absent'
Assert-Result 'failed-result' ($pass + '<test name="Failed" result="Fail"><failure><message>regression</message></failure></test>') 'Failed test'
Assert-Result 'cleanup-error' $pass 'Test infrastructure error' -Errors '<error type="test-assembly-cleanup"><failure><message>cleanup failed</message></failure></error>'
Assert-Result 'empty-report' '' 'No test results'
Assert-Result 'unknown-result' '<test name="Unknown" result="Unknown" />' 'Unknown test result'

$firstFailure = '<test name="First(format: Jet4)" type="Example.Tests" method="First" result="Fail" time="2.25"><failure><message>first failure</message><stack-trace>complete stack line 15</stack-trace></failure><output>captured diagnostic</output></test>'
$secondFailure = '<test name="Second" type="Example.Tests" method="Second" result="Fail"><failure><message>second failure</message></failure></test>'
Assert-Result 'multiple-errors' ($pass + $firstFailure + $secondFailure) 'Failed test' -Errors '<error type="cleanup"><failure><message>cleanup failure</message></failure></error>' -RequiredMethods 'Example.Tests.Absent'
foreach ($detail in 'first failure', 'second failure', 'cleanup failure', 'Required method did not pass') {
    if (-not ($summary.violations -join "`n").Contains($detail)) { throw "Missing aggregated violation: $detail" }
}
if ($summary.failures.Count -ne 2 -or $summary.failures[0].details -notmatch 'complete stack line 15' -or $summary.failures[0].output -ne 'captured diagnostic') { throw 'Complete failure diagnostics were lost.' }
if ($summary.failures[0].rerun -notmatch '--filter-method ''Example.Tests.First''' -or $markdown -notmatch 'First\(format: Jet4\)') { throw 'Focused rerun command or failing data row was lost.' }
$artifactDirectory = Join-Path $root 'artifact-rerun'
[IO.Directory]::CreateDirectory($artifactDirectory) | Out-Null
[IO.File]::WriteAllText((Join-Path $artifactDirectory 'tests.xml'), "<assemblies><assembly><collection>$firstFailure</collection></assembly></assemblies>")
try { & $helper -ResultsDirectory $artifactDirectory -RunnerDirectory "artifacts/runner's copy" } catch { }
$artifactSummary = Get-Content -LiteralPath (Join-Path $artifactDirectory 'summary.json') -Raw | ConvertFrom-Json
if ($artifactSummary.failures[0].rerun -cne "dotnet test --test-modules JetDatabaseWriter.Tests.dll --root-directory 'artifacts/runner''s copy' --filter-method 'Example.Tests.First'") { throw 'Artifact reruns must preserve the selected binary and quote paths literally.' }
Assert-Result 'invalid-duration' '<test name="Bad duration" result="Pass" time="NaN" />' 'Invalid duration'

$savedSummary = $env:GITHUB_STEP_SUMMARY
$savedCulture = [Globalization.CultureInfo]::CurrentCulture
try {
    [Globalization.CultureInfo]::CurrentCulture = [Globalization.CultureInfo]::GetCultureInfo('fr-FR')
    $env:GITHUB_STEP_SUMMARY = Join-Path $root 'github-summary.md'
    $timingDirectory = Join-Path $root 'timings'
    [IO.Directory]::CreateDirectory($timingDirectory) | Out-Null
    [IO.File]::WriteAllText((Join-Path $timingDirectory 'tests.xml'), '<assemblies><assembly time="3.5"><collection><test name="Slow &lt;script&gt;|case" type="Example.Slow" method="Case" result="Pass" time="7.25" /><test name="Fast" type="Example.Fast" method="Case" result="Pass" time="0.25" /></collection></assembly></assemblies>')
    & $helper -ResultsDirectory $timingDirectory
    $timing = Get-Content -LiteralPath (Join-Path $timingDirectory 'summary.json') -Raw | ConvertFrom-Json
    $jobSummary = Get-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Raw
    if ($timing.assemblies[0].seconds -ne 3.5 -or $timing.summedCaseSeconds -ne 7.5 -or $timing.slowestCases[0].seconds -ne 7.25 -or $timing.slowestClasses[0].type -ne 'Example.Slow') { throw 'Invariant durations or slow-case ordering were lost.' }
    if ($jobSummary -match '<script>' -or $jobSummary -notmatch '&lt;script&gt;&#124;case' -or $jobSummary -notmatch 'summed case time') { throw 'Job summary must escape data and distinguish concurrent case time.' }
    'PASS: timing summary and safe job output'
} finally { $env:GITHUB_STEP_SUMMARY = $savedSummary; [Globalization.CultureInfo]::CurrentCulture = $savedCulture }

$missing = Join-Path $root 'missing'
[IO.Directory]::CreateDirectory($missing) | Out-Null
$caught = ''
try { & $helper -ResultsDirectory $missing } catch { $caught = $_.Exception.Message }
if (-not $caught.Contains('No xUnit XML reports')) { throw "Missing reports were accepted: $caught" }
[IO.File]::WriteAllText((Join-Path $missing 'tests.xml'), '<assemblies>')
$caught = ''
try { & $helper -ResultsDirectory $missing } catch { $caught = $_.Exception.Message }
if (-not $caught.Contains('Invalid xUnit XML')) { throw "Truncated reports were accepted: $caught" }
[IO.File]::WriteAllText((Join-Path $missing 'tests.xml'), '<!DOCTYPE assemblies [<!ENTITY external SYSTEM "file:///invalid">]><assemblies>&external;</assemblies>')
$caught = ''
try { & $helper -ResultsDirectory $missing } catch { $caught = $_.Exception.Message }
if (-not $caught.Contains('Invalid xUnit XML')) { throw "DTD-bearing reports were accepted: $caught" }
'PASS: missing, truncated and DTD-bearing reports refuse'
