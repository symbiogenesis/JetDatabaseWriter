# Parse every PowerShell script and run isolated, self-contained script regression checks.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
foreach ($file in Get-ChildItem -LiteralPath $PSScriptRoot -Filter '*.ps1' -File -Recurse) {
    $parseErrors = $null
    [void][Management.Automation.Language.Parser]::ParseFile($file.FullName, [ref]$null, [ref]$parseErrors)
    if ($parseErrors.Count -gt 0) { throw "$($file.FullName): $($parseErrors.Message -join '; ')" }
}
$pwsh = Join-Path $PSHOME $(if ($IsWindows) { 'pwsh.exe' } else { 'pwsh' })
$tests = @(Get-ChildItem -LiteralPath (Join-Path $PSScriptRoot 'tests') -Filter '*.tests.ps1' -File | Sort-Object Name)
# Each check already owns a separate process and temporary fixtures. Bound startup
# concurrency and retain each process's complete output so failures stay readable.
$results = @($tests | ForEach-Object -Parallel {
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $false
    $test = $_
    try {
        $output = @(& $using:pwsh -NoProfile -File $test.FullName 2>&1)
        $exitCode = $LASTEXITCODE
    }
    catch {
        $output = @($_)
        $exitCode = 1
    }
    [pscustomobject]@{ Name = $test.Name; ExitCode = $exitCode; Output = $output }
} -ThrottleLimit 4)
foreach ($result in $results | Sort-Object Name) {
    foreach ($message in $result.Output) { Write-Host $message }
}
$failures = @($results | Where-Object ExitCode -ne 0)
if ($failures.Count -gt 0) {
    throw (($failures | Sort-Object Name | ForEach-Object { "$($_.Name) failed with exit code $($_.ExitCode)." }) -join "`n")
}
