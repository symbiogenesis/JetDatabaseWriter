# Run through x86 Windows PowerShell with DAO3.6. Stored definitions are never executed.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory
)

$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT' -or [Environment]::Is64BitProcess) {
    throw 'Run this fixture producer through x86 Windows PowerShell with DAO3.6 installed.'
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
[System.IO.Directory]::CreateDirectory($OutputDirectory) | Out-Null
foreach ($name in @('NativeSqlSpecificQueriesV1997.mdb', 'NativeSqlSpecificQueriesV2000.mdb')) {
    if (Test-Path -LiteralPath (Join-Path $OutputDirectory $name)) {
        throw "Output already exists: $name"
    }
}
$previousDirectory = [Environment]::CurrentDirectory
Push-Location -LiteralPath $OutputDirectory
try {
    [Environment]::CurrentDirectory = $OutputDirectory
    $engine = New-Object -ComObject DAO.DBEngine.36
    try {
        foreach ($entry in @(@('V1997',32),@('V2000',64))) {
            $path = Join-Path $OutputDirectory ('NativeSqlSpecificQueries' + $entry[0] + '.mdb')
            $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0', $entry[1])
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
