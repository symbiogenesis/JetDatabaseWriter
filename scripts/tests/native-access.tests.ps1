# Exercise provisioning and native runner handoff without installing Office or running tests.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$scripts = Split-Path $PSScriptRoot -Parent
foreach ($file in 'install-native-access.ps1', 'run-native-compatibility.ps1') {
    $parseErrors = $null
    $ast = [Management.Automation.Language.Parser]::ParseFile((Join-Path $scripts $file), [ref]$null, [ref]$parseErrors)
    if ($parseErrors.Count -gt 0) { throw ($parseErrors.Message -join '; ') }
    foreach ($definition in $ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
        . ([scriptblock]::Create($definition.Extent.Text))
    }
}
$root = Join-Path ([IO.Path]::GetTempPath()) "jdw-native-access-$([guid]::NewGuid().ToString('N'))"
[IO.Directory]::CreateDirectory($root) | Out-Null
$calls = [Collections.Generic.List[string]]::new()
$failure = ''
function Invoke-WebRequest { param($Uri, $OutFile); $calls.Add("download:$Uri"); [IO.File]::WriteAllText($OutFile, 'mock installer') }
function Get-FileHash { param($LiteralPath, $Algorithm); @{ Hash = $(if ($failure -eq 'hash') { 'bad' } else { 'AF1594121AB610CD8ABC3DD6FF0A504E0CBC6CE54372E5C3813926352D1179C6' }) } }
function Get-AuthenticodeSignature { param($LiteralPath); @{ Status = $(if ($failure -eq 'signature') { 'NotSigned' } else { 'Valid' }); SignerCertificate = @{ Subject = 'CN=Microsoft Corporation, O=Microsoft Corporation' } } }
function Start-Process {
    param($FilePath, $ArgumentList, $WindowStyle, [switch]$Wait, [switch]$PassThru)
    if ($WindowStyle -ne 'Hidden' -or -not $Wait -or -not $PassThru) { throw 'Provisioning children must stay hidden and finish before probe.' }
    $calls.Add("process:$FilePath")
    @{ ExitCode = $(if ($failure -eq 'process') { 1 } else { 0 }) }
}
function Assert-NativeAccessAvailable { $calls.Add('probe'); if ($failure -eq 'probe') { throw 'DAO activation failed.' } }
function Assert-Throws([scriptblock]$Action, [string]$Message) {
    $caught = ''
    try { & $Action } catch { $caught = $_.Exception.Message }
    if (-not $caught.Contains($Message)) { throw "Expected '$Message', received '$caught'." }
}

$daoModulePath = Join-Path $root 'ACEDAO.DLL'
$daoModule = @{ ModuleName = 'ACEDAO.DLL'; FileName = $daoModulePath; FileVersionInfo = @{ FileVersion = '16.0.20527.20016' } }
$receipt = Get-NativeDaoModuleReceipt @($daoModule)
if ($receipt -cne "DAO120_READY|16.0.20527.20016|$daoModulePath") { throw 'The native receipt lost the exact loaded DAO module identity.' }
Assert-Throws { Get-NativeDaoModuleReceipt @() } 'Exactly one loaded ACEDAO.DLL'
Assert-Throws { Get-NativeDaoModuleReceipt @($daoModule, $daoModule) } 'Exactly one loaded ACEDAO.DLL'
$daoModule.FileVersionInfo.FileVersion = '16.0.20400.10000'
Assert-Throws { Get-NativeDaoModuleReceipt @($daoModule) } 'Loaded ACEDAO.DLL version mismatch'
$daoModule.FileVersionInfo.FileVersion = '16.0.20527.20112'
Assert-Throws { Get-NativeDaoModuleReceipt @($daoModule) } 'Loaded ACEDAO.DLL version mismatch'
$daoModule.FileVersionInfo.FileVersion = '16.0.20527.20016'
$daoModule.FileName = 'relative/ACEDAO.DLL'
Assert-Throws { Get-NativeDaoModuleReceipt @($daoModule) } 'Loaded ACEDAO.DLL path'
$daoModule.FileName = $daoModulePath
$daoModule.ModuleName = 'MSACCESS.EXE'
Assert-Throws { Get-NativeDaoModuleReceipt @($daoModule) } 'Exactly one loaded ACEDAO.DLL'
$daoModule.ModuleName = 'ACEDAO.DLL'

if ($IsWindows) {
    # The COM probe runs in Windows PowerShell/.NET Framework, not the pwsh host.
    $policy = ${function:Get-NativeDaoModuleReceipt}.ToString()
    $frameworkChecks = "function Get-NativeDaoModuleReceipt {`n$policy`n}`n" + @'
$ErrorActionPreference = 'Stop'
$module = @{ ModuleName = 'ACEDAO.DLL'; FileName = 'C:\Program Files\Microsoft Office\root\Office16\ACEDAO.DLL'; FileVersionInfo = @{ FileVersion = '16.0.20527.20016' } }
foreach ($valid in 'C:\Program Files\Microsoft Office\root\Office16\ACEDAO.DLL', '\\server\share\ACEDAO.DLL') {
    $module.FileName = $valid
    if ((Get-NativeDaoModuleReceipt @($module)) -cne "DAO120_READY|16.0.20527.20016|$valid") { throw 'Framework policy lost the module path.' }
}
foreach ($invalid in 'relative\ACEDAO.DLL', 'C:ACEDAO.DLL', '\ACEDAO.DLL') {
    $module.FileName = $invalid
    $caught = ''
    try { Get-NativeDaoModuleReceipt @($module) | Out-Null } catch { $caught = $_.Exception.Message }
    if (-not $caught.Contains('Loaded ACEDAO.DLL path')) { throw "Framework policy accepted a nonqualified path: $invalid" }
}
$module.FileName = 'C:\Program Files\Microsoft Office\root\Office16\ACEDAO.DLL'
foreach ($invalidVersion in '16.0.20400.10000', '16.0.20527.20112') {
    $module.FileVersionInfo.FileVersion = $invalidVersion
    $caught = ''
    try { Get-NativeDaoModuleReceipt @($module) | Out-Null } catch { $caught = $_.Exception.Message }
    if (-not $caught.Contains('Loaded ACEDAO.DLL version mismatch')) { throw "Framework policy accepted another native module version: $invalidVersion" }
}
'FRAMEWORK_POLICY_OK'
'@
    $encodedChecks = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($frameworkChecks))
    $frameworkHost = Join-Path $env:windir 'System32/WindowsPowerShell/v1.0/powershell.exe'
    $frameworkOutput = @(& $frameworkHost -NoProfile -NonInteractive -EncodedCommand $encodedChecks)
    if ($LASTEXITCODE -ne 0 -or $frameworkOutput.Count -ne 1 -or $frameworkOutput[0] -cne 'FRAMEWORK_POLICY_OK') { throw 'Native module receipt policy failed in the actual Windows PowerShell probe host.' }
}

$savedEnvironment = @{}
foreach ($name in 'GITHUB_ACTIONS', 'RUNNER_ENVIRONMENT', 'RUNNER_OS', 'RUNNER_TEMP') { $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name) }
try {
    $env:GITHUB_ACTIONS = 'true'; $env:RUNNER_ENVIRONMENT = 'github-hosted'; $env:RUNNER_OS = 'Windows'; $env:RUNNER_TEMP = $root
    Install-NativeAccessRuntime
    if ($calls.Count -ne 4 -or $calls[3] -ne 'probe') { throw "Incomplete install/probe sequence: $calls" }
    $calls.Clear(); $env:RUNNER_ENVIRONMENT = 'self-hosted'
    Assert-Throws { Install-NativeAccessRuntime } 'disposable GitHub-hosted Windows runner'
    if ($calls.Count -ne 0) { throw 'Refused host still performed provisioning.' }
    $env:RUNNER_ENVIRONMENT = 'github-hosted'
    foreach ($case in @{ name = 'hash'; message = 'digest mismatch' }, @{ name = 'signature'; message = 'signature mismatch' }, @{ name = 'process'; message = 'extraction failed' }, @{ name = 'probe'; message = 'DAO activation failed' }) {
        $failure = $case.name; $calls.Clear()
        Assert-Throws { Install-NativeAccessRuntime } $case.message
        if ($case.name -in 'hash', 'signature' -and @($calls | Where-Object { $_.StartsWith('process:') }).Count -ne 0) { throw 'Untrusted installer executed.' }
    }
} finally {
    foreach ($name in $savedEnvironment.Keys) { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name]) }
}
$failure = ''
$methods = @(Get-NativeCompatibilityMethods)
if ($methods.Count -lt 16 -or @($methods | Sort-Object -Unique).Count -ne $methods.Count -or $methods -match 'Dao36|Jet3|\*') { throw 'Native admission must name distinct DAO120-only methods.' }
foreach ($method in $methods) {
    $last = $method.LastIndexOf('.')
    $className = $method.Substring(0, $last)
    $methodName = $method.Substring($last + 1)
    $typePath = ($className -replace '^JetDatabaseWriter.Tests\.', '').Replace('.', '/') + '.cs'
    $source = [IO.File]::ReadAllText((Join-Path $scripts "../tests/JetDatabaseWriter.Tests/$typePath"))
    if ($source -notmatch ('\b' + [regex]::Escape($methodName) + '\s*\(')) { throw "Native method no longer exists: $method" }
}
$runnerDirectory = Join-Path $root 'runner'
[IO.Directory]::CreateDirectory($runnerDirectory) | Out-Null
[IO.File]::WriteAllText((Join-Path $runnerDirectory 'JetDatabaseWriter.Tests.dll'), 'mock assembly')
$capturedArguments = @()
function dotnet { $script:capturedArguments = @($args); $global:LASTEXITCODE = 0 }
function Assert-NativeAccessAvailable { }
function Assert-NativeResults { param($ResultsDirectory, $Methods); if ($Methods.Count -ne $methods.Count) { throw 'Runner did not retain its complete inventory.' } }
Invoke-NativeCompatibility -RunnerDirectory $runnerDirectory -ResultsDirectory (Join-Path $root 'results')
$command = $capturedArguments -join ' '
if ($command -notmatch '^test --test-modules JetDatabaseWriter.Tests.dll --root-directory ' -or $command -notmatch '--filter-method ' -or $command -match '\b(build|restore|publish)\b') { throw "Native lane must execute exact analyzed binaries: $command" }
foreach ($method in $methods) { if ($method -cnotin $capturedArguments) { throw "Native runner dropped required method $method" } }
function dotnet { $global:LASTEXITCODE = 7 }
Assert-Throws { Invoke-NativeCompatibility -RunnerDirectory $runnerDirectory -ResultsDirectory (Join-Path $root 'failed') } 'Native compatibility tests failed'
'PASS: native provisioning integrity, host refusal, inventory and analyzed-binary execution'
