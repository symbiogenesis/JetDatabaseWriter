# Provision only a disposable hosted runner; local Access installations stay user-managed.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'

function Get-NativeDaoModuleReceipt([object[]] $Modules) {
    $loaded = @($Modules | Where-Object { $_.ModuleName -ieq 'ACEDAO.DLL' })
    if ($loaded.Count -ne 1) { throw 'Exactly one loaded ACEDAO.DLL is required after DAO120 activation.' }
    $module = $loaded[0]
    $version = $module.FileVersionInfo.FileVersion
    if ($version -cne '16.0.20527.20112') { throw "Loaded ACEDAO.DLL version mismatch: '$version'." }
    # This policy also runs in Windows PowerShell's .NET Framework child.
    # Rooted-relative and drive-relative paths cannot round-trip as absolute names.
    $absolutePath = $false
    try { $absolutePath = [IO.Path]::IsPathRooted($module.FileName) -and [IO.Path]::GetFullPath($module.FileName) -ceq $module.FileName }
    catch { $absolutePath = $false }
    if (-not $absolutePath -or $module.FileName.IndexOfAny([char[]]"|`r`n") -ge 0) { throw 'Loaded ACEDAO.DLL path cannot identify a native module receipt.' }
    return "DAO120_READY|$version|$($module.FileName)"
}

function Assert-NativeAccessAvailable {
    $accessPath = Join-Path $env:ProgramFiles 'Microsoft Office/root/Office16/MSACCESS.EXE'
    if (-not [IO.File]::Exists($accessPath)) { throw 'Pinned Microsoft Access Runtime is missing.' }
    $version = [Diagnostics.FileVersionInfo]::GetVersionInfo($accessPath).FileVersion
    if ($version -cne '16.0.20527.20112') { throw "Microsoft Access Runtime version mismatch: '$version'." }
    $hostPath = Join-Path $env:windir 'System32/WindowsPowerShell/v1.0/powershell.exe'
    $policy = ${function:Get-NativeDaoModuleReceipt}.ToString()
    $probe = "function Get-NativeDaoModuleReceipt {`n$policy`n}`n" + @'
$ErrorActionPreference = 'Stop'
$engine = New-Object -ComObject DAO.DBEngine.120
try {
    $nativeProcess = [Diagnostics.Process]::GetCurrentProcess()
    try { Get-NativeDaoModuleReceipt @($nativeProcess.Modules) }
    finally { $nativeProcess.Dispose() }
}
finally { [void][Runtime.InteropServices.Marshal]::ReleaseComObject($engine) }
'@
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($probe))
    $output = @(& $hostPath -NoProfile -NonInteractive -EncodedCommand $encoded)
    $receipts = @($output | Where-Object { $_ -cmatch '^DAO120_READY\|16\.0\.20527\.20112\|[^|\r\n]+$' })
    if ($LASTEXITCODE -ne 0 -or $receipts.Count -ne 1) { throw 'Microsoft DAO120 activation or loaded-module pin failed.' }
    Write-Output "Native Microsoft Access Runtime $version; $($output -join ' ')"
}

function Install-NativeAccessRuntime {
    if ($env:GITHUB_ACTIONS -cne 'true' -or $env:RUNNER_ENVIRONMENT -cne 'github-hosted' -or $env:RUNNER_OS -cne 'Windows' -or -not $env:RUNNER_TEMP) {
        throw 'Access provisioning requires a disposable GitHub-hosted Windows runner.'
    }
    $directory = Join-Path $env:RUNNER_TEMP 'jdw-access-runtime'
    [IO.Directory]::CreateDirectory($directory) | Out-Null
    $installer = Join-Path $directory 'officedeploymenttool.exe'
    Invoke-WebRequest 'https://download.microsoft.com/download/6c1eeb25-cf8b-41d9-8d0d-cc1dbc032140/officedeploymenttool_20430-20146.exe' -OutFile $installer
    if ((Get-FileHash -LiteralPath $installer -Algorithm SHA256).Hash -cne 'AF1594121AB610CD8ABC3DD6FF0A504E0CBC6CE54372E5C3813926352D1179C6') { throw 'Office Deployment Tool digest mismatch.' }
    $signature = Get-AuthenticodeSignature -LiteralPath $installer
    if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notlike 'CN=Microsoft Corporation,*') { throw 'Office Deployment Tool signature mismatch.' }
    $deployment = Join-Path $directory 'deployment'
    [IO.Directory]::CreateDirectory($deployment) | Out-Null
    $extract = Start-Process $installer -ArgumentList @('/quiet', "/extract:`"$deployment`"") -WindowStyle Hidden -Wait -PassThru
    if ($extract.ExitCode -ne 0) { throw "Office Deployment Tool extraction failed: $($extract.ExitCode)." }
    $configuration = Join-Path $deployment 'access-runtime.xml'
    [IO.File]::WriteAllText($configuration, '<Configuration><Add OfficeClientEdition="64" Channel="Current" Version="16.0.20527.20112"><Product ID="AccessRuntimeRetail"><Language ID="en-us"/><ExcludeApp ID="OneDrive"/></Product></Add><Display Level="None" AcceptEULA="TRUE"/></Configuration>')
    $install = Start-Process (Join-Path $deployment 'setup.exe') -ArgumentList @('/configure', "`"$configuration`"") -WindowStyle Hidden -Wait -PassThru
    if ($install.ExitCode -notin 0, 3010) { throw "Microsoft Access Runtime installation failed: $($install.ExitCode)." }
    Assert-NativeAccessAvailable
}

if ($MyInvocation.InvocationName -ne '.') { Install-NativeAccessRuntime }
