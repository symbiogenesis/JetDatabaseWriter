# Run with a Windows PowerShell host that can activate the selected native DAO engine.
# An entirely new synthetic workgroup is created with Microsoft Jet OLEDB 4.0.
# Use a short output path for legacy DAO workgroup resolution.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$OutputDirectory,
    [ValidateSet('DAO.DBEngine.120', 'DAO.DBEngine.36')][string]$EngineProgId = 'DAO.DBEngine.36'
)

$ErrorActionPreference = 'Stop'
$output = [System.IO.Path]::GetFullPath($OutputDirectory)
[System.IO.Directory]::CreateDirectory($output) | Out-Null
$original = Join-Path $output 'NativeJet4Workgroup.mdb'
$changed = Join-Path $output 'NativeJet4WorkgroupChanged.mdb'
foreach ($path in @($original, $changed)) {
    if (Test-Path -LiteralPath $path) { throw "Output already exists: $path" }
}

$workgroup = Join-Path $output 'NativeJetWorkgroup.mdw'
if (Test-Path -LiteralPath $workgroup) { throw "Workgroup output already exists: $workgroup" }
$catalog = New-Object -ComObject ADOX.Catalog
try {
    $catalog.Create("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=$workgroup;Jet OLEDB:Create System Database=True;") | Out-Null
    $catalog.ActiveConnection.Close()
} finally {
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($catalog) | Out-Null
}
$engine = $null
$workspace = $null
$database = $null
try {
    $engine = New-Object -ComObject $EngineProgId
    $engine.SystemDB = $workgroup
    $workspace = $engine.CreateWorkspace('Setup', 'Admin', '', 2)
    $workspace.Users.Append($workspace.CreateUser('NativeOwner', 'NativeOwnerPID', 'Owner123'))
    $admins = $workspace.Groups.Item('Admins')
    $admins.Users.Append($admins.CreateUser('NativeOwner'))
    $workspace.Close()
    $workspace = $engine.CreateWorkspace('SyntheticOwner', 'NativeOwner', 'Owner123', 2)
    $database = $workspace.CreateDatabase($original, ';LANGID=0x0409;CP=1252;COUNTRY=0;pwd=Native123', 66)
    $database.Execute('CREATE TABLE T (Id LONG, Label TEXT(80))', 128)
    $database.Execute("INSERT INTO T VALUES(7,'Native encrypted row')", 128)
    $database.Containers('Tables').Documents.Refresh()
    if ($database.Containers('Tables').Documents('T').Owner -ne 'NativeOwner') {
        throw 'Native fixture does not belong to the synthetic workgroup owner.'
    }
    $database.Close()
    $database = $null
    Copy-Item -LiteralPath $original -Destination $changed
    $database = $workspace.OpenDatabase($changed, $true, $false, ';PWD=Native123')
    $database.NewPassword('Native123', 'Changed123')
    $database.Containers('Tables').Documents.Refresh()
    if ($database.Containers('Tables').Documents('T').Owner -ne 'NativeOwner') {
        throw 'Native password change lost the synthetic workgroup owner.'
    }
    $database.Close()
    $database = $null
    foreach ($path in @($original, $changed)) {
        $hash = [System.Security.Cryptography.SHA256]::Create()
        $stream = [System.IO.File]::OpenRead($path)
        try { Write-Output ($path + ' SHA256=' + [BitConverter]::ToString($hash.ComputeHash($stream)).Replace('-', '')) }
        finally { $stream.Dispose(); $hash.Dispose() }
    }
} finally {
    if ($null -ne $database) { $database.Close() }
    if ($null -ne $workspace) { $workspace.Close() }
    if ($null -ne $engine) { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
}
# Retain the isolated workgroup beside the fixtures for native owner/permission probes.
# Its synthetic account password is Owner123; database passwords are Native123 and Changed123.
