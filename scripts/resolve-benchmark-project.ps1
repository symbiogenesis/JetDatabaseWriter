# Resolve the project in each revision independently so published baselines can use the earlier layout.
[CmdletBinding()]
param([Parameter(Mandatory)][string] $Repository)

$ErrorActionPreference = 'Stop'
foreach ($project in 'benchmarks/JetDatabaseWriter.Benchmarks/JetDatabaseWriter.Benchmarks.csproj', 'JetDatabaseWriter.Benchmarks/JetDatabaseWriter.Benchmarks.csproj') {
    if (Test-Path -LiteralPath (Join-Path $Repository $project) -PathType Leaf) {
        return $project
    }
}
throw "Missing benchmark project in '$Repository'."
