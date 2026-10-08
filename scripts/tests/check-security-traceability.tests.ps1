# Synthetic repositories exercise the traceability checker without a build or external tools.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$fixture = Join-Path $repoRoot ('artifacts/script-tests/security-' + [guid]::NewGuid().ToString('N'))
foreach ($directory in 'scripts', 'docs', 'JetDatabaseWriter.Tests/nested', 'JetDatabaseWriter.Tests/bin/deep', 'JetDatabaseWriter.Tests/OBJ/deep') {
    [IO.Directory]::CreateDirectory((Join-Path $fixture $directory)) | Out-Null
}
$checker = Join-Path $fixture 'scripts/check-security-traceability.ps1'
[IO.File]::Copy((Join-Path $repoRoot 'scripts/check-security-traceability.ps1'), $checker)
$documentPath = Join-Path $fixture 'docs/cve-vulnerability-analysis.md'
$sourcePath = Join-Path $fixture 'JetDatabaseWriter.Tests/nested/Regression.cs'
$document = @"
| [CVE-2020-1000](https://example.test/one) | Analogue: ``Regression.Case`` |
| [CVE-2020-1001](https://example.test/two) | Analogue: ``Regression.Case`` |
| [CVE-2020-1002](https://example.test/three) | N/A: This example does not apply. |
"@
$source = @"
public sealed class Regression
{
    [Fact]
    [Trait("CveAnalogue", "CVE-2020-1000")]
    [Trait("CveAnalogue", "CVE-2020-1001")]
    public async Task Case() { }
}
"@
[IO.File]::WriteAllText((Join-Path $fixture 'JetDatabaseWriter.Tests/Ordinary.cs'), 'public class Ordinary { }')
foreach ($directory in 'bin', 'OBJ') {
    [IO.File]::WriteAllText((Join-Path $fixture "JetDatabaseWriter.Tests/$directory/deep/Invalid.cs"), '[Trait("CveAnalogue", "CVE-9999-9999")]')
}

function Assert-Traceability([string] $Name, [string] $Document, [string] $Source, [string] $ExpectedError) {
    [IO.File]::WriteAllText($documentPath, $Document)
    [IO.File]::WriteAllText($sourcePath, $Source)
    $caught = $null
    try { $result = & $checker } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        if (-not $caught -or $caught -notlike "*$ExpectedError*") { throw "$Name failed: expected '$ExpectedError', got '$caught'." }
    } elseif ($caught) {
        throw "$Name failed: $caught"
    } elseif ($result -ne 'Security traceability: 3 CVEs, 2 canonical analogue mappings, 1 explicit N/A dispositions.') {
        throw "$Name failed: unexpected summary '$result'."
    }
    "PASS: $Name"
}

Assert-Traceability 'nested sources, stacked traits, and pruned build outputs' $document $source
Assert-Traceability 'duplicate inventory' ($document + "`n" + $document) $source 'Duplicate inventory row'
Assert-Traceability 'unlisted mention' ($document + "`nCVE-2020-9999") $source 'mentioned without an inventory disposition'
Assert-Traceability 'missing mapping' $document 'public class Ordinary { }' 'without a matching CveAnalogue trait'
Assert-Traceability 'misplaced trait' $document ($source.Replace('public async Task Case()', 'private async Task Case()')) 'Unrecognized CveAnalogue attribute placement'
Assert-Traceability 'missing Fact' $document ($source.Replace('[Fact]', '')) 'no adjacent Fact/Theory'
Assert-Traceability 'mismatched method' $document ($source.Replace('Task Case()', 'Task Wrong()')) 'in code but'
Assert-Traceability 'missing disposition' ($document.Replace('N/A: This example does not apply.', 'Unexplained.')) $source 'needs an explicit Analogue'
Assert-Traceability 'missing inventory' 'No table.' $source 'No individually sourced CVE inventory rows'
