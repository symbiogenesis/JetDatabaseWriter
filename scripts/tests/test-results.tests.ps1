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
    "PASS: $Name"
}

$pass = '<test name="Case" type="Example.Tests" method="Case" result="Pass" />'
$dao = '<test name="Dao" type="Example.Dao" method="Native" result="Skip"><reason><![CDATA[Requires Microsoft Access (DAO.DBEngine.120)]]></reason></test>'
$explicit = '<test name="Fuzz" type="JetDatabaseWriter.Tests.Fuzz.AccessReaderFuzzTests" method="FuzzAccessReader" result="NotRun" />'
Assert-Result 'ordinary' ($pass + $dao + $explicit)
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
