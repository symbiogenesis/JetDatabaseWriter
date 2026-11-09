# xUnit's XML keeps explicit NotRun outcomes separate from conditional Skip outcomes.
# https://xunit.net/docs/format-xml-v2
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $ResultsDirectory,
    [switch] $RequireNoSkips,
    [string[]] $RequiredMethods = @(),
    [string] $RunnerDirectory = ''
)
$ErrorActionPreference = 'Stop'
[void][IO.Directory]::CreateDirectory($ResultsDirectory)
$reports = @(Get-ChildItem -LiteralPath $ResultsDirectory -Filter '*.xml' -File -Recurse | Sort-Object FullName)
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
$counts = [ordered]@{ Pass = 0; Fail = 0; Skip = 0; NotRun = 0; Unknown = 0 }
$violations = [Collections.Generic.List[string]]::new()
$failures = [Collections.Generic.List[object]]::new()
$cases = [Collections.Generic.List[object]]::new()
$assemblies = [Collections.Generic.List[object]]::new()
$guardedSkips = 0
$explicitNotRun = 0
if ($reports.Count -eq 0) { $violations.Add("No xUnit XML reports in '$ResultsDirectory'.") }

function Get-Seconds([Xml.XmlElement] $Element) {
    if (-not $Element.HasAttribute('time')) { return $null }
    $seconds = [decimal]0
    if (-not [decimal]::TryParse($Element.GetAttribute('time'), [Globalization.NumberStyles]::AllowDecimalPoint, [Globalization.CultureInfo]::InvariantCulture, [ref]$seconds) -or $seconds -lt 0) {
        $violations.Add("Invalid duration for '$($Element.GetAttribute('name'))'.")
        return $null
    }
    return $seconds
}

function Get-Rerun([string] $Identity) {
    $method = "'" + $Identity.Replace("'", "''") + "'"
    if ($RunnerDirectory) {
        $runner = "'" + $RunnerDirectory.Replace("'", "''") + "'"
        return "dotnet test --test-modules JetDatabaseWriter.Tests.dll --root-directory $runner --filter-method $method"
    }
    return "dotnet test --project tests/JetDatabaseWriter.Tests -c Release --filter-method $method"
}

function ConvertTo-Cell([string] $Value) {
    return [Net.WebUtility]::HtmlEncode($Value).Replace('|', '&#124;').Replace("`r", '').Replace("`n", '<br>')
}

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
    } catch { $violations.Add("Invalid xUnit XML '$($report.FullName)': $($_.Exception.Message)"); continue }
    finally { if ($reader) { $reader.Dispose() } }
    if ($document.DocumentElement.Name -cne 'assemblies') { $violations.Add("Invalid xUnit XML root in '$($report.FullName)'."); continue }
    foreach ($assembly in $document.SelectNodes('/assemblies/assembly')) {
        $assemblies.Add([ordered]@{ name = $assembly.GetAttribute('name'); framework = $assembly.GetAttribute('target-framework'); seconds = (Get-Seconds $assembly) })
    }
    $errors = @($document.SelectNodes('/assemblies/assembly/errors/error'))
    foreach ($errorNode in $errors) { $violations.Add("Test infrastructure error in '$($report.FullName)': $($errorNode.InnerText)") }
    $tests = @($document.SelectNodes('/assemblies/assembly/collection/test'))
    if ($tests.Count -eq 0) { $violations.Add("No test results in '$($report.FullName)'.") }
    foreach ($test in $tests) {
        $identity = $test.GetAttribute('type') + '.' + $test.GetAttribute('method')
        $name = $test.GetAttribute('name')
        $result = $test.GetAttribute('result')
        switch -CaseSensitive ($result) {
            Pass { [void]$passedMethods.Add($identity) }
            Fail {
                $violations.Add("Failed test '$name': $($test.InnerText)")
                $failureNode = $test.SelectSingleNode('failure')
                $outputNode = $test.SelectSingleNode('output')
                $failures.Add([ordered]@{
                    name = $name; method = $identity; report = $report.Name
                    details = $(if ($failureNode) { $failureNode.InnerText } else { $test.InnerText })
                    output = $(if ($outputNode) { $outputNode.InnerText } else { '' })
                    rerun = (Get-Rerun $identity)
                })
            }
            Skip {
                $reasonNode = $test.SelectSingleNode('reason')
                $reason = if ($reasonNode) { $reasonNode.InnerText } else { '' }
                $knownExplicit = $identity -cin $explicitMethods -and $reason -ceq 'Not run (due to explicit test filtering)'
                if ($knownExplicit) { $explicitNotRun++ }
                elseif ($reason -cin $daoReasons) { $guardedSkips++ }
                if ($RequireNoSkips -or ($reason -cnotin $daoReasons -and -not $knownExplicit)) {
                    $violations.Add("Unexpected skipped test '$name': $reason")
                }
            }
            NotRun {
                if ($identity -cin $explicitMethods) { $explicitNotRun++ }
                if ($RequireNoSkips -or $identity -cnotin $explicitMethods) { $violations.Add("Unexpected not-run test '$name' ($identity).") }
            }
            default { $violations.Add("Unknown test result '$result' for '$name'."); $result = 'Unknown' }
        }
        $counts[$result]++
        $cases.Add([pscustomobject]@{ name = $name; type = $test.GetAttribute('type'); method = $identity; seconds = (Get-Seconds $test) })
    }
}
foreach ($method in $RequiredMethods) {
    if (-not $passedMethods.Contains($method)) { $violations.Add("Required method did not pass: $method") }
}
if ($counts.Pass -eq 0) { $violations.Add('No passing test results were recorded.') }
$slowestCases = @($cases | Where-Object { $null -ne $_.seconds } | Sort-Object @{ Expression = 'seconds'; Descending = $true }, name | Select-Object -First 20)
$slowestClasses = @($cases | Group-Object type | ForEach-Object {
    [pscustomobject]@{ type = $_.Name; cases = $_.Count; summedCaseSeconds = [decimal](($_.Group | Measure-Object seconds -Sum).Sum) }
} | Sort-Object @{ Expression = 'summedCaseSeconds'; Descending = $true }, type | Select-Object -First 20)
$summary = [ordered]@{
    schemaVersion = 1; name = [IO.Path]::GetFileName([IO.Path]::GetFullPath($ResultsDirectory).TrimEnd([IO.Path]::DirectorySeparatorChar))
    status = $(if ($violations.Count) { 'failure' } else { 'success' }); counts = $counts
    guardedSkips = $guardedSkips; explicitNotRun = $explicitNotRun
    assemblies = @($assemblies.ToArray()); summedCaseSeconds = [decimal](($cases | Measure-Object seconds -Sum).Sum)
    slowestCases = $slowestCases; slowestClasses = $slowestClasses
    failures = @($failures.ToArray()); violations = @($violations.ToArray())
}
$summaryPath = Join-Path $ResultsDirectory 'summary.json'
[IO.File]::WriteAllText($summaryPath, ($summary | ConvertTo-Json -Depth 12))
$markdown = [Collections.Generic.List[string]]::new()
$markdown.Add('## Tests: ' + (ConvertTo-Cell $summary.name))
$markdown.Add('')
$markdown.Add("$($counts.Pass) passed; $($counts.Fail) failed; $guardedSkips DAO guarded skips; $explicitNotRun explicit tests not run; $($violations.Count) validation errors.")
$markdown.Add('')
foreach ($assembly in $assemblies) {
    if ($null -ne $assembly.seconds) { $markdown.Add("Assembly elapsed: $($assembly.seconds.ToString([Globalization.CultureInfo]::InvariantCulture)) s ($(ConvertTo-Cell $assembly.framework)).") }
}
$markdown.Add("Concurrent tests: summed case time $($summary.summedCaseSeconds.ToString([Globalization.CultureInfo]::InvariantCulture)) s; class totals below are summed case time, not wall time.")
$markdown.Add('')
$markdown.Add('Complete failures, captured output and validation errors are retained in summary.json beside the original XML. Reruns require the same runner artifact and runtime environment as the original job.')
if ($violations.Count) {
    $markdown.Add('')
    $markdown.Add('### Validation errors (first 20)')
    $markdown.Add('')
    foreach ($violation in $violations | Select-Object -First 20) {
        $excerpt = if ($violation.Length -gt 500) { $violation.Substring(0, 500) + '...' } else { $violation }
        $markdown.Add('- ' + (ConvertTo-Cell $excerpt))
    }
}
if ($failures.Count) {
    $markdown.Add('')
    $markdown.Add('### Focused failure reruns (first 20)')
    $markdown.Add('')
    foreach ($failed in $failures | Select-Object -First 20) { $markdown.Add('<pre>' + [Net.WebUtility]::HtmlEncode($failed.name + "`n" + $failed.rerun) + '</pre>') }
}
$markdown.Add('')
$markdown.Add('### Slowest cases')
$markdown.Add('')
$markdown.Add('| Seconds | Case |')
$markdown.Add('| ---: | --- |')
foreach ($case in $slowestCases) { $markdown.Add('| ' + $case.seconds.ToString('0.###', [Globalization.CultureInfo]::InvariantCulture) + ' | ' + (ConvertTo-Cell $case.name) + ' |') }
$markdown.Add('')
$markdown.Add('### Slowest classes by summed case time')
$markdown.Add('')
$markdown.Add('| Seconds | Cases | Class |')
$markdown.Add('| ---: | ---: | --- |')
foreach ($class in $slowestClasses) { $markdown.Add('| ' + $class.summedCaseSeconds.ToString('0.###', [Globalization.CultureInfo]::InvariantCulture) + ' | ' + $class.cases + ' | ' + (ConvertTo-Cell $class.type) + ' |') }
$markdownText = ($markdown -join "`r`n") + "`r`n"
[IO.File]::WriteAllText((Join-Path $ResultsDirectory 'summary.md'), $markdownText)
if ($env:GITHUB_STEP_SUMMARY) { [IO.File]::AppendAllText($env:GITHUB_STEP_SUMMARY, $markdownText) }
Write-Output "xUnit results: $($counts.Pass) passed, $($counts.Fail) failed, $guardedSkips DAO guarded skips, $explicitNotRun explicit tests not run. Details: $summaryPath"
if ($violations.Count) {
    $excerpts = @($violations | Select-Object -First 20 | ForEach-Object { if ($_.Length -gt 500) { $_.Substring(0, 500) + '...' } else { $_ } })
    throw "$($violations.Count) test-result validation errors (complete details in '$summaryPath'):`n$($excerpts -join "`n")"
}
