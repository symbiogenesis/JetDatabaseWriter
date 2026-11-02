# Verify the shipping license inventory without building or accessing package feeds.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$project = [xml][IO.File]::ReadAllText((Join-Path $repository 'src/JetDatabaseWriter/JetDatabaseWriter.csproj'))
foreach ($name in 'LICENSE', 'LICENSE-APACHE-2.0.txt', 'THIRD-PARTY-NOTICES.md') {
    $item = @($project.SelectNodes('/Project/ItemGroup/None') | Where-Object { $_.Include -eq ('..\..\' + $name) })
    if ($item.Count -ne 1 -or $item[0].Pack -ne 'true' -or $item[0].PackagePath -ne '\') {
        throw "The NuGet package must include the complete $name at its root."
    }
    if (-not (Test-Path -LiteralPath (Join-Path $repository $name) -PathType Leaf)) { throw "Missing shipping license: $name" }
}
$apache = [IO.File]::ReadAllText((Join-Path $repository 'LICENSE-APACHE-2.0.txt'))
foreach ($section in 'TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION', '4. Redistribution.', '9. Accepting Warranty or Additional Liability.', 'END OF TERMS AND CONDITIONS') {
    if (-not $apache.Contains($section)) { throw "The Apache license is incomplete: missing $section" }
}
$notices = [IO.File]::ReadAllText((Join-Path $repository 'THIRD-PARTY-NOTICES.md'))
foreach ($resource in $project.SelectNodes('/Project/ItemGroup/EmbeddedResource')) {
    $name = [IO.Path]::GetFileName($resource.Include.Replace('\', '/'))
    if (-not $notices.Contains($name)) { throw "Missing attribution for the shipped resource $name." }
}
Write-Host 'Shipping license declarations and resource attribution passed.'
