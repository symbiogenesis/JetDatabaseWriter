# Execute the workflow's patch coordinator against a synthetic checkout and Git doubles.
[CmdletBinding()]
param([string] $OutputDirectory)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('jdw-sdk-patch-tests-' + [Guid]::NewGuid().ToString('N'))
}
$testRoot = [IO.Path]::GetFullPath($OutputDirectory)
$fixture = Join-Path $testRoot 'fixture'
[IO.Directory]::CreateDirectory((Join-Path $fixture 'Library')) | Out-Null
[IO.Directory]::CreateDirectory((Join-Path $fixture 'scripts/AotSmoke')) | Out-Null
[IO.File]::WriteAllText((Join-Path $fixture 'JetDatabaseWriter.slnx'), '<Solution><Project Path="Library/Library.csproj" /></Solution>')
$expectedPaths = @('global.json', 'Library/packages.lock.json', 'Library/packages.netstandard.lock.json', 'scripts/AotSmoke/packages.native.lock.json', 'scripts/AotSmoke/packages.seed.lock.json', 'scripts/AotSmoke/packages.trimmed.lock.json')
foreach ($path in $expectedPaths) { [IO.File]::WriteAllText((Join-Path $fixture $path), 'generated SDK file') }
[IO.File]::WriteAllText((Join-Path $fixture 'Unrelated.cs'), '// Other work must be excluded')
[IO.File]::WriteAllText((Join-Path $fixture 'Library/packages.unrelated.lock.json'), 'Other lock must be excluded')
$context = @{
    Calls = [Collections.Generic.List[object]]::new()
    Visible = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
}
foreach ($path in $expectedPaths | Where-Object { $_ -ne 'scripts/AotSmoke/packages.native.lock.json' }) { [void]$context.Visible.Add($path) }

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Get-Paths([string[]] $Arguments) {
    $separator = [Array]::IndexOf($Arguments, '--')
    # PowerShell consumes the -- terminator when invoking this function double.
    if ($separator -lt 0) {
        for ($index = 0; $index -lt $Arguments.Length; $index++) {
            if ($Arguments[$index] -notin 'add', 'diff' -and -not $Arguments[$index].StartsWith('-', [StringComparison]::Ordinal)) {
                $separator = $index - 1
                break
            }
        }
    }
    return $Arguments[($separator + 1)..($Arguments.Length - 1)]
}

function git {
    $arguments = [string[]]$args
    $context.Calls.Add($arguments)
    $global:LASTEXITCODE = 0
    if ($arguments -contains '--intent-to-add') {
        foreach ($path in Get-Paths $arguments) { [void]$context.Visible.Add($path) }
    } elseif ($arguments -contains '--binary') {
        $output = @($arguments | Where-Object { $_.StartsWith('--output=', [StringComparison]::Ordinal) })[0].Substring(9)
        # An ordinary diff sees generated untracked locks only after intent-to-add.
        $paths = @(Get-Paths $arguments)
        if ($paths -contains ':(glob)**/packages*.lock.json') { $paths = $expectedPaths }
        $included = @($paths | Where-Object { $context.Visible.Contains($_) })
        [IO.File]::WriteAllText($output, ($included -join "`r`n"))
    } elseif ($arguments -notcontains '--check') { throw 'Unexpected Git command.' }
}

$workflow = [IO.File]::ReadAllText((Join-Path $PSScriptRoot '../../.github/workflows/sdk-lock-refresh.yml')).Replace("`r`n", "`n")
$step = [regex]::Match($workflow, '(?ms)^      - name: Create SDK and lock patch\n(?<body>.*?)(?=^      - |\z)').Groups['body'].Value
$command = [regex]::Match($step, '(?ms)^        run: \|\n(?<command>.*)$').Groups['command'].Value
Assert-Check ([bool]$command) 'The SDK patch coordinator is missing.'
$command = [regex]::Replace($command, '(?m)^          ', '')
$savedEnvironment = @{}
foreach ($name in 'RUNNER_TEMP', 'GITHUB_SHA', 'GITHUB_REPOSITORY', 'GITHUB_RUN_ID', 'SDK_VERSION') {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
}
Push-Location $fixture
try {
    $env:RUNNER_TEMP = $testRoot
    $env:GITHUB_SHA = 'a' * 40
    $env:GITHUB_REPOSITORY = 'owner/repo'
    $env:GITHUB_RUN_ID = '42'
    $env:SDK_VERSION = '10.0.402'
    & ([scriptblock]::Create($command))
} finally {
    Pop-Location
    foreach ($name in $savedEnvironment.Keys) { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name]) }
}
$patch = [IO.File]::ReadAllText((Join-Path $testRoot 'sdk-lock-patch/sdk-locks.patch'))
Assert-Check ($patch.Contains('scripts/AotSmoke/packages.native.lock.json')) 'The hosted patch omitted a newly generated smoke lockfile.'
$adds = @($context.Calls | Where-Object { $_ -contains '--intent-to-add' })
Assert-Check ($adds.Count -eq 1) 'Generated SDK files must be made visible with one intent-to-add operation.'
Assert-Check ((@(Get-Paths $adds[0] | Sort-Object) -join ',') -eq (($expectedPaths | Sort-Object) -join ',')) 'Intent-to-add included arbitrary source or lock files.'
$diffs = @($context.Calls | Where-Object { $_ -contains '--binary' })
Assert-Check ((@(Get-Paths $diffs[0] | Sort-Object) -join ',') -eq (($expectedPaths | Sort-Object) -join ',')) 'SDK patch export must name only the expected generated files.'
Assert-Check (-not $patch.Contains('Unrelated') -and -not $patch.Contains('packages.unrelated')) 'The SDK patch included other work.'
Write-Host "SDK patch includes new locks and excludes unrelated files. Artifacts: $testRoot"
