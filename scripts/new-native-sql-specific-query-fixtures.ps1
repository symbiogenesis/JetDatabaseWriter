# Run through a bitness-matched PowerShell host. Stored definitions are never executed.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory,

    [ValidateSet('Dao36', 'Dao120')]
    [string]$Provider = 'Dao36'
)

$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT' -or ($Provider -eq 'Dao36' -and [Environment]::Is64BitProcess)) {
    throw 'Run through a Windows PowerShell host matching the provider bitness; DAO3.6 requires x86.'
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
[System.IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
$formats = if ($Provider -eq 'Dao36') {
    [pscustomobject]@{ Name = 'V1997'; Version = 32; Extension = '.mdb' }
    [pscustomobject]@{ Name = 'V2000'; Version = 64; Extension = '.mdb' }
} else {
    [pscustomobject]@{ Name = 'V2007'; Version = 128; Extension = '.accdb' }
}
foreach ($entry in $formats) {
    $name = 'NativeSqlSpecificQueries' + $entry.Name + $entry.Extension
    if (Test-Path -LiteralPath (Join-Path $OutputDirectory $name)) {
        throw "Output already exists: $name"
    }
}
$previousDirectory = [Environment]::CurrentDirectory
Push-Location -LiteralPath $OutputDirectory
try {
    [Environment]::CurrentDirectory = $OutputDirectory
    $engine = New-Object -ComObject $(if ($Provider -eq 'Dao36') { 'DAO.DBEngine.36' } else { 'DAO.DBEngine.120' })
    try {
        foreach ($entry in $formats) {
            $path = Join-Path $OutputDirectory ('NativeSqlSpecificQueries' + $entry.Name + $entry.Extension)
            $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0', $entry.Version)
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG, [Note] LONG, [Keep] LONG)', 128)
                $db.Execute('INSERT INTO [T] ([Id],[Note],[Keep]) VALUES (1,2,9)', 128)
                $query = $db.CreateQueryDef('CreateDefinition', 'CREATE TABLE [NeverCreated] ([Id] LONG, [Note] LONG)')
                $query.Properties.Append($query.CreateProperty('Description', 12, ('Inert stored DDL; ' * 160)))
                $query.Close()
                foreach ($returnsRecords in @($true, $false)) {
                    $name = if ($returnsRecords) { 'PassThroughSelect' } else { 'PassThroughAction' }
                    $query = $db.CreateQueryDef($name)
                    $query.Connect = 'ODBC;DSN=JDW_Inert_NotInstalled;DATABASE=JDW_MetadataOnly;'
                    $query.ReturnsRecords = $returnsRecords
                    $query.ODBCTimeout = 17
                    if ($returnsRecords) {
                        $query.MaxRecords = 23
                        $query.SQL = 'SELECT [Id], [Note] FROM [T] WHERE [Id] = 1'
                    } else {
                        $query.SQL = 'DELETE FROM [T] WHERE [Id] = 1'
                    }
                    $query.Properties.Append($query.CreateProperty('Description', 12, ('Inert pass-through metadata; ' * 120)))
                    $query.Close()
                }
            } finally { $db.Close() }
            $db = $engine.OpenDatabase($path, $false, $true)
            try {
                foreach ($queryName in @('CreateDefinition','PassThroughSelect','PassThroughAction')) {
                    $query = $db.QueryDefs($queryName)
                    $expectedType = switch ($queryName) { 'CreateDefinition' { 96 }; 'PassThroughSelect' { 112 }; 'PassThroughAction' { 144 } }
                    if ($query.Type -ne $expectedType) { throw ('Wrong query type: ' + $queryName + '=' + $query.Type) }
                    $query.Close()
                }
            } finally { $db.Close() }
            Get-FileHash -LiteralPath $path -Algorithm SHA256
        }
    } finally { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }
} finally {
    [Environment]::CurrentDirectory = $previousDirectory
    Pop-Location
}
