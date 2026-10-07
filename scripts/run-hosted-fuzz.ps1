# Runs explicit fuzz cases in separate, bounded processes using CI-built binaries.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $Runner,
    [ValidateRange(1, 512)][int] $Cases = 32,
    [ValidateRange(1, 120)][int] $TimeoutSeconds = 30,
    [ValidateRange(64, 2048)][int] $MemoryLimitMiB = 512,
    [int] $Seed = 20261006,
    [string] $OutputDirectory = 'artifacts/fuzz'
)
$ErrorActionPreference = 'Stop'
$runnerPath = (Resolve-Path -LiteralPath $Runner).Path
$repoRoot = Split-Path $PSScriptRoot -Parent
$outputPath = [IO.Path]::GetFullPath($OutputDirectory)
[IO.Directory]::CreateDirectory($outputPath) | Out-Null
$corpus = @(Get-ChildItem -LiteralPath (Join-Path $repoRoot 'JetDatabaseWriter.Tests/Databases') -Recurse -File |
    Where-Object { $_.Extension -in '.mdb', '.accdb' -and $_.Length -le 16MB } | Sort-Object FullName)
if ($corpus.Count -eq 0) { throw 'No bounded database corpus found.' }
$random = [Random]::new($Seed)
$methods = @{
    Reader = 'JetDatabaseWriter.Tests.Fuzz.AccessReaderFuzzTests.FuzzAccessReader'
    Writer = 'JetDatabaseWriter.Tests.Fuzz.AccessWriterFuzzTests.FuzzAccessWriter'
    Text = 'JetDatabaseWriter.Tests.Fuzz.DelimitedTextReaderFuzzTests.FuzzDelimitedTextReader'
}
$failures = 0
$manifest = [Collections.Generic.List[object]]::new()
foreach ($kind in @('Reader', 'Writer', 'Text')) {
    for ($case = 0; $case -lt $Cases; $case++) {
        $caseName = '{0}-{1:D4}' -f $kind, $case
        $source = $null
        if ($kind -eq 'Reader') {
            $fixture = $corpus[($case + [Math]::Abs([long]$Seed)) % $corpus.Count]
            $source = [IO.Path]::GetRelativePath($repoRoot, $fixture.FullName)
            $bytes = [IO.File]::ReadAllBytes($fixture.FullName)
            if ($case % 4 -eq 1) {
                $length = $random.Next($bytes.Length + 1)
                $truncated = [byte[]]::new($length)
                [Array]::Copy($bytes, $truncated, $length)
                $bytes = $truncated
            } elseif ($case % 4 -ge 2) {
                for ($mutation = 0; $mutation -lt 16; $mutation++) {
                    $position = $random.Next($bytes.Length)
                    $bytes[$position] = [byte]$random.Next(256)
                }
            }
        } else {
            $bytes = [byte[]]::new($random.Next(1, 4097))
            $random.NextBytes($bytes)
        }
        $inputPath = Join-Path $outputPath "$caseName.bin"
        $xmlPath = Join-Path $outputPath "$caseName.xml"
        $stdoutPath = Join-Path $outputPath "$caseName.stdout.log"
        $stderrPath = Join-Path $outputPath "$caseName.stderr.log"
        [IO.File]::WriteAllBytes($inputPath, $bytes)
        $hash = (Get-FileHash -LiteralPath $inputPath -Algorithm SHA256).Hash
        $arguments = @('-method', $methods[$kind], '-explicit', 'only', '-parallelMode', 'none', '-noColor', '-result-xml', ('"{0}"' -f $xmlPath))
        $environment = @{ JETDATABASEWRITER_FUZZ_INPUT = $inputPath; DOTNET_GCHeapHardLimit = ('0x{0:X}' -f ($MemoryLimitMiB * 1MB)) }
        $process = Start-Process -FilePath $runnerPath -ArgumentList $arguments -WorkingDirectory (Split-Path $runnerPath) -Environment $environment -WindowStyle Hidden -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
        $timer = [Diagnostics.Stopwatch]::StartNew()
        $status = 'Passed'
        while (-not $process.WaitForExit(250)) {
            $process.Refresh()
            if ($timer.Elapsed.TotalSeconds -gt $TimeoutSeconds) { $status = 'Timeout'; break }
            if ($process.PrivateMemorySize64 -gt ($MemoryLimitMiB * 1MB)) { $status = 'MemoryLimit'; break }
        }
        if ($status -ne 'Passed') {
            if (-not $process.HasExited) { $process.Kill($true) }
            $process.WaitForExit()
        } elseif ($process.ExitCode -ne 0) {
            $status = 'Failed'
        } elseif (-not (Test-Path -LiteralPath $xmlPath)) {
            $status = 'MissingResult'
        } else {
            [xml]$result = [IO.File]::ReadAllText($xmlPath)
            $tests = @($result.SelectNodes('//test'))
            if ($tests.Count -ne 1 -or $tests[0].result -ne 'Pass') { $status = 'UnexpectedResult' }
        }
        $process.Dispose()
        $manifest.Add([pscustomobject]@{ Case = $caseName; Seed = $Seed; Source = $source; SHA256 = $hash; Status = $status; Seconds = $timer.Elapsed.TotalSeconds })
        Write-Host "$caseName $status"
        if ($status -eq 'Passed') {
            Remove-Item -LiteralPath $inputPath
        } else {
            $failures++
        }
    }
}
[IO.File]::WriteAllText((Join-Path $outputPath 'manifest.json'), ($manifest | ConvertTo-Json -Depth 4))
if ($failures -gt 0) { throw "$failures fuzz cases failed; retained inputs and logs are in $outputPath." }
