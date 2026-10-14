<#
.SYNOPSIS
Checks an unpublished candidate on a hosted runner and preserves formatter fixes as a patch.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [ValidatePattern('^[0-9a-f]{40}$')] [string] $BaseSha,
    [Parameter(Mandatory)] [string] $EncodedPatch,
    [Parameter(Mandatory)] [string] $OutputDirectory
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repoRoot = Split-Path $PSScriptRoot -Parent
if (-not [IO.Path]::IsPathFullyQualified($OutputDirectory)) { throw 'OutputDirectory must be absolute.' }
$output = [IO.Path]::GetFullPath($OutputDirectory)
[IO.Directory]::CreateDirectory($output) | Out-Null
$patchPath = Join-Path $output 'input.patch'
[IO.File]::WriteAllBytes($patchPath, [Convert]::FromBase64String($EncodedPatch))
$configuration = $env:Configuration

function Invoke-CandidateStep([string] $Name, [string[]] $Arguments) {
    Write-Host "$Name : dotnet $($Arguments -join ' ')"
    & dotnet @Arguments *> (Join-Path $output ($Name + '.log'))
    $code = $LASTEXITCODE
    Get-Content -LiteralPath (Join-Path $output ($Name + '.log')) | Write-Host
    return $code
}

Push-Location $repoRoot
try {
    git -C $repoRoot merge-base --is-ancestor $BaseSha origin/main
    if ($LASTEXITCODE -ne 0) { throw 'The candidate base must belong to published main history.' }
    git -C $repoRoot checkout --detach $BaseSha
    if ($LASTEXITCODE -ne 0) { throw 'Cannot check out the candidate base.' }
    git -C $repoRoot apply --check -- $patchPath
    if ($LASTEXITCODE -ne 0) { throw 'The candidate patch does not apply cleanly to its base.' }
    git -C $repoRoot apply --index -- $patchPath
    if ($LASTEXITCODE -ne 0) { throw 'Cannot apply the candidate patch.' }

    $code = Invoke-CandidateStep 'restore' @('restore', 'JetDatabaseWriter.slnx', '--locked-mode')
    if ($code -ne 0) { throw "Locked restore failed with exit code $code." }
    $code = Invoke-CandidateStep 'compile' @('build', 'JetDatabaseWriter.slnx', '-c', 'Release', '--no-restore', '-p:RunAnalyzers=false', ('-bl:' + (Join-Path $output 'compile.binlog')))
    if ($code -ne 0) { throw "Compiler check failed with exit code $code." }

    $changed = @(git -C $repoRoot diff --name-only --diff-filter=ACMR $BaseSha -- | Where-Object { $_.EndsWith('.cs', [StringComparison]::OrdinalIgnoreCase) })
    if ($LASTEXITCODE -ne 0) { throw 'Cannot enumerate changed source files.' }
    $env:Configuration = 'Release'
    $formatFailed = $false
    if ($changed.Count -gt 0) {
        foreach ($kind in 'whitespace', 'style', 'analyzers') {
            $arguments = @('format', $kind, 'JetDatabaseWriter.slnx', '--no-restore', '--verbosity', 'diagnostic', '--report', (Join-Path $output ($kind + '.json')))
            if ($kind -ne 'whitespace') { $arguments += @('--severity', 'info') }
            $arguments += @('--include') + $changed
            $code = Invoke-CandidateStep $kind $arguments
            if ($code -ne 0) { $formatFailed = $true }
        }
    }

    # The SDK formatter is supplementary. A strict Release build covers every
    # solution target, including conditional source and diagnostics without a fixer.
    $code = Invoke-CandidateStep 'strict' @('build', 'JetDatabaseWriter.slnx', '-c', 'Release', '--no-restore', ('-bl:' + (Join-Path $output 'strict.binlog')))
    if ($code -ne 0) { throw "Strict candidate build failed with exit code $code." }
    if ($formatFailed) { throw 'A formatter pass failed; inspect the retained reports and logs.' }
    Write-Host 'Candidate compilation and analysis passed. Review format-fixes.patch before integrating; main still requires CI.'
} finally {
    $env:Configuration = $configuration
    git -C $repoRoot diff --binary ('--output=' + (Join-Path $output 'format-fixes.patch')) --
    git -C $repoRoot diff --binary $BaseSha ('--output=' + (Join-Path $output 'candidate.patch')) --
    Pop-Location
}
