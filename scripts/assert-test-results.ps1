# xUnit's XML keeps explicit NotRun outcomes separate from conditional Skip outcomes.
# https://xunit.net/docs/format-xml-v2
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $ResultsDirectory,
    [switch] $RequireNoSkips,
    [string[]] $RequiredMethods = @()
)
$ErrorActionPreference = 'Stop'
$reports = @(Get-ChildItem -LiteralPath $ResultsDirectory -Filter '*.xml' -File -Recurse)
if ($reports.Count -eq 0) { throw "No xUnit XML reports in '$ResultsDirectory'." }
$explicitMethods = @(
    'JetDatabaseWriter.Tests.Fuzz.AccessReaderFuzzTests.FuzzAccessReader'
    'JetDatabaseWriter.Tests.Fuzz.AccessWriterFuzzTests.FuzzAccessWriter'
    'JetDatabaseWriter.Tests.Fuzz.DelimitedTextReaderFuzzTests.FuzzDelimitedTextReader'
)
$daoReasons = @(
    'Requires Microsoft Access (DAO.DBEngine.120)'
    'Requires Microsoft Access (DAO.DBEngine.120) and JETDATABASEWRITER_RUN_KNOWN_ACCESS_COMPAT_GAPS=1'
)
$passedMethods = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$counts = @{ Pass = 0; Skip = 0; NotRun = 0 }
foreach ($report in $reports) {
    $settings = [Xml.XmlReaderSettings]::new()
    $settings.DtdProcessing = [Xml.DtdProcessing]::Prohibit
    $settings.XmlResolver = $null
    $reader = $null
    try {
        $reader = [Xml.XmlReader]::Create($report.FullName, $settings)
        $document = [Xml.XmlDocument]::new()
        $document.XmlResolver = $null
        $document.Load($reader)
    } catch { throw "Invalid xUnit XML '$($report.FullName)': $($_.Exception.Message)" }
    finally { if ($reader) { $reader.Dispose() } }
    if ($document.DocumentElement.Name -cne 'assemblies') { throw "Invalid xUnit XML root in '$($report.FullName)'." }
    $errors = @($document.SelectNodes('/assemblies/assembly/errors/error'))
    if ($errors.Count -gt 0) { throw "Test infrastructure error in '$($report.FullName)': $($errors[0].InnerText)" }
    $tests = @($document.SelectNodes('/assemblies/assembly/collection/test'))
    if ($tests.Count -eq 0) { throw "No test results in '$($report.FullName)'." }
    foreach ($test in $tests) {
        $identity = $test.GetAttribute('type') + '.' + $test.GetAttribute('method')
        $name = $test.GetAttribute('name')
        $result = $test.GetAttribute('result')
        switch -CaseSensitive ($result) {
            Pass { [void]$passedMethods.Add($identity) }
            Fail { throw "Failed test '$name': $($test.InnerText)" }
            Skip {
                $reasonNode = $test.SelectSingleNode('reason')
                $reason = if ($reasonNode) { $reasonNode.InnerText } else { '' }
                $knownExplicit = $identity -cin $explicitMethods -and $reason -ceq 'Not run (due to explicit test filtering)'
                if ($RequireNoSkips -or ($reason -cnotin $daoReasons -and -not $knownExplicit)) {
                    throw "Unexpected skipped test '$name': $reason"
                }
            }
            NotRun {
                if ($RequireNoSkips -or $identity -cnotin $explicitMethods) { throw "Unexpected not-run test '$name' ($identity)." }
            }
            default { throw "Unknown test result '$result' for '$name'." }
        }
        $counts[$result]++
    }
}
foreach ($method in $RequiredMethods) {
    if (-not $passedMethods.Contains($method)) { throw "Required method did not pass: $method" }
}
if ($counts.Pass -eq 0) { throw 'No passing test results were recorded.' }
Write-Output "Validated xUnit results: $($counts.Pass) passed, $($counts.Skip) guarded skips, $($counts.NotRun) explicit tests not run."
