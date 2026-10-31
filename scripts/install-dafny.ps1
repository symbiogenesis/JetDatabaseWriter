# Installs a checksum-pinned, self-contained verifier outside the source tree.
[CmdletBinding()]
param([Parameter(Mandatory)][string] $Destination)
$ErrorActionPreference = 'Stop'

function Assert-DafnyArchive([string] $ArchivePath, [string] $ExpectedHash) {
    $actual = (Get-FileHash -LiteralPath $ArchivePath -Algorithm SHA256).Hash
    if ($ExpectedHash -notmatch '^[0-9a-f]{64}$' -or $actual -ne $ExpectedHash) { throw "Dafny archive SHA256 mismatch: expected $ExpectedHash, got $actual" }
}

function Install-PinnedDafny([string] $RepositoryRoot, [string] $Destination) {
    if ($Destination -match '%[^%]+%' -or -not [IO.Path]::IsPathFullyQualified($Destination)) { throw 'The Dafny destination must be absolute with no unresolved environment variables.' }
    $destinationPath = [IO.Path]::GetFullPath($Destination)
    $root = [IO.Path]::GetFullPath($RepositoryRoot).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    if ($destinationPath.Equals($root, [StringComparison]::OrdinalIgnoreCase) -or $destinationPath.StartsWith($root + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Install Dafny outside the repository.' }
    if (Test-Path -LiteralPath $destinationPath) { throw 'The Dafny destination must not already exist.' }
    $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    $supported = ($IsWindows -and $architecture -in [Runtime.InteropServices.Architecture]::X64, [Runtime.InteropServices.Architecture]::Arm64) -or
        ($IsLinux -and $architecture -eq [Runtime.InteropServices.Architecture]::X64)
    if (-not $supported) { throw 'The pinned toolchain supports Windows x64 (including Windows Arm64 emulation) and Ubuntu x64 only.' }
    $manifest = [IO.File]::ReadAllText((Join-Path $RepositoryRoot 'verification/config/dafny-toolchain.json')) | ConvertFrom-Json
    if ($manifest.schemaVersion -ne 1 -or $manifest.version -ne '4.11.0') { throw 'Unsupported Dafny toolchain manifest.' }
    $asset = if ($IsWindows) { $manifest.assets.windows } else { $manifest.assets.linux }
    if ($asset.url -notmatch '^https://github\.com/dafny-lang/dafny/releases/download/v4\.11\.0/dafny-4\.11\.0-x64-(windows-2022|ubuntu-22\.04)\.zip$') { throw 'Unexpected Dafny release asset URL.' }
    if ($asset.sha256 -notmatch '^[0-9a-f]{64}$') { throw 'Invalid Dafny archive digest in manifest.' }
    $scratch = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-dafny-install-' + [guid]::NewGuid().ToString('N'))
    [void][IO.Directory]::CreateDirectory($scratch)
    $archive = Join-Path $scratch 'dafny.zip'
    # Keep failed downloads and their hash evidence in this unique temporary lane.
    Invoke-WebRequest -Uri $asset.url -OutFile $archive -TimeoutSec 180 -MaximumRetryCount 2 | Out-Null
    Assert-DafnyArchive $archive $asset.sha256
    [IO.Compression.ZipFile]::ExtractToDirectory($archive, $destinationPath)
    if (-not $IsWindows) {
        # Zip extraction on .NET does not restore Unix execution permission.
        $zip = [IO.Compression.ZipFile]::OpenRead($archive)
        try {
            foreach ($entry in $zip.Entries) {
                $mode = ($entry.ExternalAttributes -shr 16) -band 511
                if (-not $entry.FullName.EndsWith('/') -and ($mode -band 73) -ne 0) {
                    [IO.File]::SetUnixFileMode((Join-Path $destinationPath $entry.FullName), [IO.UnixFileMode]$mode)
                }
            }
        } finally { $zip.Dispose() }
    }
    $executable = Join-Path $destinationPath $(if ($IsWindows) { 'dafny/dafny.exe' } else { 'dafny/dafny' })
    if (-not [IO.File]::Exists($executable)) { throw 'The pinned archive did not contain the expected Dafny executable.' }
    [IO.File]::WriteAllText((Join-Path $scratch 'verified-archive.txt'), "SHA256 $($asset.sha256)`n$($asset.url)`nInstalled $destinationPath`n")
    Write-Output $executable
}

Install-PinnedDafny (Split-Path $PSScriptRoot -Parent) $Destination
