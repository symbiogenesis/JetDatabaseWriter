# Checks the CVE inventory against canonical xUnit CveAnalogue traits without building.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path $PSScriptRoot -Parent
$document = [IO.File]::ReadAllText((Join-Path $repoRoot 'docs/cve-vulnerability-analysis.md'))
$inventory = @{}
$rows = [regex]::Matches($document, '(?m)^\| \[(?<cve>CVE-\d{4}-\d+)\]\([^\r\n|]+\) \|(?<content>[^\r\n]+)\|\s*$')
foreach ($row in $rows) {
    $cve = $row.Groups['cve'].Value
    if ($inventory.ContainsKey($cve)) { throw "Duplicate inventory row: $cve" }
    $content = $row.Groups['content'].Value
    $analogue = [regex]::Match($content, 'Analogue: `(?<method>\w+\.\w+)`')
    if ($analogue.Success) {
        $inventory[$cve] = $analogue.Groups['method'].Value
    } elseif ($content -match 'N/A: .{10,}') {
        $inventory[$cve] = $null
    } else {
        throw "$cve needs an explicit Analogue method or an N/A reason."
    }
}
if ($inventory.Count -eq 0) { throw 'No individually sourced CVE inventory rows found.' }
foreach ($mention in [regex]::Matches($document, 'CVE-\d{4}-\d+')) {
    if (-not $inventory.ContainsKey($mention.Value)) {
        throw "$($mention.Value) is mentioned without an inventory disposition."
    }
}

$mapped = @{}
$traitPattern = '\[Trait\("CveAnalogue", "(?<cve>CVE-\d{4}-\d+)"\)\](?=\s*(?:\[Trait\("CveAnalogue", "CVE-\d{4}-\d+"\)\]\s*)*public\s+(?:async\s+)?(?:Task|ValueTask|void)\s+(?<method>\w+)\s*\()'
$testFiles = Get-ChildItem -LiteralPath (Join-Path $repoRoot 'JetDatabaseWriter.Tests') -Filter '*.cs' -File -Recurse |
    Where-Object { $_.FullName -notmatch '[\\/](bin|obj)[\\/]' }
foreach ($file in $testFiles) {
    $source = [IO.File]::ReadAllText($file.FullName)
    $traits = [regex]::Matches($source, $traitPattern)
    if ([regex]::Matches($source, '\[Trait\("CveAnalogue",').Count -ne $traits.Count) {
        throw "Unrecognized CveAnalogue attribute placement in $($file.FullName); put canonical traits immediately before the method."
    }
    $class = [regex]::Match($source, '\bpublic\s+(?:sealed\s+)?class\s+(?<name>\w+)').Groups['name'].Value
    foreach ($trait in $traits) {
        $cve = $trait.Groups['cve'].Value
        $method = "$class.$($trait.Groups['method'].Value)"
        if (-not $inventory.ContainsKey($cve)) { throw "$cve on $method has no inventory row." }
        if ($mapped.ContainsKey($cve)) { throw "$cve has more than one canonical regression." }
        if ($inventory[$cve] -ne $method) { throw "$cve maps to $method in code but '$($inventory[$cve])' in the document." }
        $mapped[$cve] = $method
    }
}
foreach ($cve in $inventory.Keys) {
    if ($null -ne $inventory[$cve] -and -not $mapped.ContainsKey($cve)) {
        throw "$cve declares a regression without a matching CveAnalogue trait."
    }
}
Write-Output ("Security traceability: {0} CVEs, {1} canonical analogue mappings, {2} explicit N/A dispositions." -f $inventory.Count, $mapped.Count, ($inventory.Count - $mapped.Count))
