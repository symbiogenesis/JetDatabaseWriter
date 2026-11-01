# Guard the independent hosted fuzz gate, dependency cache and retained reproductions.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$workflow = [IO.File]::ReadAllText((Join-Path $repository '.github/workflows/fuzz.yml')).Replace("`r`n", "`n")

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Assert-Workflow([string] $Workflow) {
    $prerequisite = [regex]::Match($Workflow, '(?ms)^  prerequisite:\n(?<job>.*?)(?=^  [\w-]+:\n|\z)').Groups['job'].Value
    $fuzz = [regex]::Match($Workflow, '(?ms)^  fuzz:\n(?<job>.*?)(?=^  [\w-]+:\n|\z)').Groups['job'].Value
    Assert-Contract ($prerequisite.Contains("if: github.ref == 'refs/heads/main'") -and $prerequisite.Contains('./scripts/assert-ci-success.ps1') -and $fuzz.Contains('needs: prerequisite')) 'Fuzzing must require successful exact-main CI before starting its runners.'
    Assert-Contract ($Workflow.Contains('DOTNET_NOLOGO: true') -and $Workflow.Contains('DOTNET_CLI_TELEMETRY_OPTOUT: true')) 'Hosted fuzz builds must use the common .NET environment.'
    $checkouts = [regex]::Matches($Workflow, '(?ms)^      - (?:name: [^\n]+\n        )?uses: actions/checkout@[^\n]+\n(?<step>.*?)(?=^      - |\z)')
    Assert-Contract ($checkouts.Count -eq 2) 'Both fuzz jobs must check out the dispatched revision.'
    foreach ($checkout in $checkouts) {
        Assert-Contract ($checkout.Groups['step'].Value.Contains('persist-credentials: false')) 'Fuzz checkouts must not persist repository credentials.'
    }
    $setup = [regex]::Match($fuzz, '(?ms)^      - (?:name: [^\n]+\n        )?uses: actions/setup-dotnet@[^\n]+\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($setup.Contains('global-json-file: global.json') -and $setup.Contains('cache: true') -and $setup.Contains("cache-dependency-path: '**/packages.lock.json'")) 'Fuzz runners must cache their locked NuGet dependencies with the pinned SDK.'
    Assert-Contract ($fuzz.Contains('tf: [net10.0, net11.0]') -and $fuzz.Contains('JDW_EXPECTED_RUNTIME_MAJOR:') -and $fuzz.Contains('DOTNET_ROLL_FORWARD_TO_PRERELEASE:')) 'Fuzzing must retain both explicitly selected runtimes.'
    Assert-Contract ($fuzz.Contains('dotnet restore tests/JetDatabaseWriter.Tests --locked-mode') -and $fuzz.Contains('dotnet build tests/JetDatabaseWriter.Tests -c Release --no-restore -f net10.0 -p:RunAnalyzers=false')) 'Fuzz runners must build locked Release inputs without repeating the CI analyzers.'
    $upload = [regex]::Match($fuzz, '(?ms)^      - name: Retain reproductions and logs\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    Assert-Contract ($upload.Contains('if: always()') -and $upload.Contains('name: fuzz-${{ matrix.tf }}') -and $upload.Contains('path: artifacts/fuzz/') -and $upload.Contains('if-no-files-found: error')) 'Each fuzz runtime must retain diagnostics on failure and report missing reproductions.'
}

function Assert-Refusal([string] $Name, [string] $Candidate, [string] $Expected) {
    $caught = $null
    try { Assert-Workflow $Candidate } catch { $caught = $_.Exception.Message }
    Assert-Contract ($caught -and $caught.Contains($Expected)) "$Name did not refuse for the expected reason: $caught"
    "PASS: $Name"
}

Assert-Workflow $workflow
Assert-Refusal 'missing main guard' ($workflow.Replace("if: github.ref == 'refs/heads/main'", 'if: true')) 'successful exact-main CI'
Assert-Refusal 'missing prerequisite' ($workflow.Replace('needs: prerequisite', 'needs: removed')) 'successful exact-main CI'
Assert-Refusal 'persisted credentials' ($workflow.Replace('persist-credentials: false', 'persist-credentials: true')) 'persist repository credentials'
Assert-Refusal 'uncached dependencies' ($workflow.Replace('cache: true', 'cache: false')) 'cache their locked NuGet dependencies'
Assert-Refusal 'unlocked dependency cache' ($workflow.Replace("cache-dependency-path: '**/packages.lock.json'", "cache-dependency-path: '**/*.csproj'")) 'cache their locked NuGet dependencies'
Assert-Refusal 'repeated analyzers' ($workflow.Replace(' -p:RunAnalyzers=false', '')) 'without repeating the CI analyzers'
Assert-Refusal 'missing runtime leg' ($workflow.Replace('tf: [net10.0, net11.0]', 'tf: [net10.0]')) 'both explicitly selected runtimes'
Assert-Refusal 'lost failure diagnostics' ($workflow.Replace('if: always()', 'if: success()')) 'retain diagnostics on failure'
Assert-Refusal 'missing reproductions ignored' ($workflow.Replace('if-no-files-found: error', 'if-no-files-found: warn')) 'report missing reproductions'
Write-Output 'Hosted fuzz workflow contracts passed.'
