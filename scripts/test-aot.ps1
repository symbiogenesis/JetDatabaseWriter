# Publishes and executes a real scaffold-generated consumer under trimming and NativeAOT.
[CmdletBinding()]
param(
    [string] $OutputDirectory
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repository = Split-Path -Parent $PSScriptRoot
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $repository ('artifacts/aot-smoke/' + [Guid]::NewGuid().ToString('N'))
}
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

function Invoke-DotNet {
    param([string[]] $Arguments)
    & dotnet @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet $($Arguments -join ' ') failed with exit code $LASTEXITCODE."
    }
}

Push-Location $repository
try {
    $project = Join-Path $repository 'scripts/AotSmoke/AotSmoke.csproj'
    $seed = Join-Path $OutputDirectory 'seed.accdb'
    $build = Join-Path $OutputDirectory 'build'
    Invoke-DotNet @('run', '--project', $project, '-c', 'Release', '-p:SmokeMode=seed', '-p:RestoreLockedMode=true', '--artifacts-path', $build, '--', $seed)
    foreach ($kind in @('classes', 'records')) {
        $models = Join-Path $OutputDirectory ($kind + '-models')
        $scaffoldArguments = @('run', '--project', 'JetDatabaseWriter.Scaffold', '-c', 'Release', '-p:RestoreLockedMode=true', '--artifacts-path', $build, '--', $seed, '--output', $models)
        if ($kind -eq 'records') {
            $scaffoldArguments += '--records'
        }
        Invoke-DotNet $scaffoldArguments
        foreach ($mode in @('trimmed', 'native')) {
            $publish = Join-Path $OutputDirectory ($kind + '-' + $mode)
            Invoke-DotNet @('publish', $project, '-c', 'Release', '-f', 'net10.0', ('-p:SmokeMode=' + $mode), ('-p:GeneratedModels=' + $models), '-p:RestoreLockedMode=true', '--artifacts-path', $build, '-o', $publish)
            $database = Join-Path $OutputDirectory ($kind + '-' + $mode + '.accdb')
            Copy-Item -LiteralPath $seed -Destination $database
            & (Join-Path $publish 'AotSmoke.exe') $database
            if ($LASTEXITCODE -ne 0) {
                throw "$kind/$mode consumer failed with exit code $LASTEXITCODE."
            }
        }
    }
    Write-Host "Trimming and NativeAOT consumers passed. Outputs: $OutputDirectory"
}
finally {
    Pop-Location
}
