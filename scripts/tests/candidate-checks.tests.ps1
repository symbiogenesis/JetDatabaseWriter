# Candidate checks use command doubles: no builds, formatting or GitHub dispatches run.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$testRoot = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-candidate-tests-' + [guid]::NewGuid().ToString('N'))
$scripts = Join-Path $testRoot 'fixture/scripts'
[IO.Directory]::CreateDirectory($scripts) | Out-Null
$subject = Join-Path $scripts 'run-candidate-checks.ps1'
Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../run-candidate-checks.ps1') -Destination $subject
$candidateContext = [pscustomobject]@{ Calls = [Collections.Generic.List[object]]::new(); Failure = ''; Ancestor = $true; ExportFails = $false }

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function git {
    $arguments = [string[]]$args
    $candidateContext.Calls.Add([pscustomobject]@{ Tool = 'git'; Arguments = $arguments })
    $global:LASTEXITCODE = 0
    if ($arguments -contains 'merge-base') { $global:LASTEXITCODE = if ($candidateContext.Ancestor) { 0 } else { 1 } }
    elseif ($arguments -contains '--name-only') {
        Assert-Check ($arguments -contains '-z') 'Changed paths must bypass Git quoting for Unicode and spaces.'
        $path = @($arguments | Where-Object { $_.StartsWith('--output=', [StringComparison]::Ordinal) })[0].Substring(9)
        [IO.File]::WriteAllText($path, "JetDatabaseWriter/Changed.cs$([char]0)JetDatabaseWriter/Café File.cs$([char]0)", [Text.UTF8Encoding]::new($false))
    }
    elseif ($arguments -contains 'diff') {
        $output = @($arguments | Where-Object { $_.StartsWith('--output=', [StringComparison]::Ordinal) })
        if ($output.Count) { [IO.File]::WriteAllText($output[0].Substring(9), 'Reviewable patch') }
        if ($candidateContext.ExportFails) { $global:LASTEXITCODE = 7 }
    }
    elseif ($arguments -contains 'apply') {
        Assert-Check ([IO.File]::ReadAllText($arguments[-1]) -eq "diff --git a/test b/test`n") 'The patch was not decoded exactly.'
    }
}

function dotnet {
    $arguments = [string[]]$args
    $candidateContext.Calls.Add([pscustomobject]@{ Tool = 'dotnet'; Arguments = $arguments; Configuration = $env:Configuration })
    $stage = if ($arguments[0] -eq 'restore') { 'restore' }
        elseif ($arguments -contains '-p:RunAnalyzers=false') { 'compile' }
        elseif ($arguments[0] -eq 'format') { 'format' }
        else { 'strict' }
    $global:LASTEXITCODE = if ($candidateContext.Failure -eq $stage) { 7 } else { 0 }
}

function Invoke-Scenario([string] $Name, [string] $Failure = '', [bool] $Ancestor = $true, [string] $EncodedPatch = '', [bool] $ExportFails = $false) {
    $candidateContext.Calls.Clear()
    $candidateContext.Failure = $Failure
    $candidateContext.Ancestor = $Ancestor
    $candidateContext.ExportFails = $ExportFails
    if (-not $EncodedPatch) { $EncodedPatch = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("diff --git a/test b/test`n")) }
    $caught = $null
    $configuration = $env:Configuration
    try { & $subject -BaseSha ('a' * 40) -EncodedPatch $EncodedPatch -OutputDirectory (Join-Path $testRoot $Name) }
    catch { $caught = $_.Exception.Message }
    Assert-Check ($env:Configuration -eq $configuration) "$Name did not restore the caller's Configuration."
    return $caught
}

$errorText = Invoke-Scenario 'pass'
Assert-Check (-not $errorText) "Passing commands failed: $errorText"
$commands = @($candidateContext.Calls | Where-Object Tool -eq 'dotnet')
Assert-Check ($commands.Count -eq 6) 'Expected locked restore, compiler pass, three formatter passes, and strict compile.'
Assert-Check ($commands[0].Arguments -contains '--locked-mode') 'Candidate restore must stay locked.'
Assert-Check ($commands[1].Arguments -contains '-p:RunAnalyzers=false') 'Fast compiler feedback must precede analysis.'
Assert-Check ($commands[-1].Arguments -notcontains '-p:RunAnalyzers=false') 'Final candidate compile must retain analyzers.'
foreach ($command in $commands | Select-Object -Skip 1) {
    if ($command.Arguments[0] -eq 'build') {
        Assert-Check ($command.Arguments -contains '--no-restore' -and $command.Arguments -contains 'Release') 'Both compiler passes must cover the Release solution without restoring again.'
    } else {
        Assert-Check ($command.Arguments -contains '--no-restore' -and $command.Arguments -contains '--include' -and $command.Arguments -contains 'JetDatabaseWriter/Changed.cs') 'Format only changed source files without restoring.'
        Assert-Check ($command.Arguments -contains 'JetDatabaseWriter/Café File.cs') 'Unicode and spaced file names must remain single formatter arguments.'
        Assert-Check ($command.Configuration -eq 'Release') 'Formatter workspace must use Release configuration.'
    }
}
Assert-Check ([IO.File]::Exists((Join-Path $testRoot 'pass/format-fixes.patch'))) 'Formatter changes must remain reviewable.'
Assert-Check ([IO.File]::Exists((Join-Path $testRoot 'pass/candidate.patch'))) 'The complete tested candidate patch must remain available.'

foreach ($failure in 'restore', 'compile', 'strict', 'format') {
    $errorText = Invoke-Scenario $failure $failure
    Assert-Check ([bool]$errorText) "$failure falsely passed."
    Assert-Check ([IO.File]::Exists((Join-Path $testRoot "$failure/candidate.patch"))) "$failure lost its patch diagnostics."
}
$errorText = Invoke-Scenario 'unpublished-base' -Ancestor $false
Assert-Check ($errorText.Contains('published main history')) 'An unpublished base must be refused.'
Assert-Check (@($candidateContext.Calls | Where-Object Tool -eq 'dotnet').Count -eq 0) 'An invalid base must be refused before any build.'
$errorText = Invoke-Scenario 'invalid-patch' -EncodedPatch 'invalid!'
Assert-Check ([bool]$errorText) 'Malformed base64 must be refused.'
Assert-Check (@($candidateContext.Calls | Where-Object Tool -eq 'dotnet').Count -eq 0) 'Malformed input must be refused before any build.'
$errorText = Invoke-Scenario 'failed-export' -ExportFails $true
Assert-Check ($errorText.Contains('Cannot export')) 'Successful compiler checks must not conceal lost review patches.'
$candidateContext.ExportFails = $false
$candidateContext.Calls.Clear()
$candidateContext.Ancestor = $true
& $subject -BaseSha ('a' * 40) -EncodedPatch ([Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("diff --git a/test b/test`n"))) -OutputDirectory (Join-Path $testRoot 'prepare') -PrepareOnly
Assert-Check (@($candidateContext.Calls | Where-Object Tool -eq 'dotnet').Count -eq 0) 'Preparing a candidate must allow SDK installation before any dotnet command.'
$candidateContext.Calls.Clear()
$candidateContext.Failure = ''
& $subject -BaseSha ('a' * 40) -OutputDirectory (Join-Path $testRoot 'check') -CheckOnly
Assert-Check (@($candidateContext.Calls | Where-Object { $_.Tool -eq 'git' -and $_.Arguments -contains 'apply' }).Count -eq 0) 'The prepared candidate must not be applied again.'
Write-Host "All candidate checks passed. Artifacts: $testRoot"
