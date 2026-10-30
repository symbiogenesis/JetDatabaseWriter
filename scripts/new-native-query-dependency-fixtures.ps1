# Run through x86 Windows PowerShell with DAO3.6. Saved queries are never executed.
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
foreach ($name in @('NativeQueryDependenciesV1997.mdb', 'NativeQueryDependenciesV2000.mdb')) {
    if (Test-Path -LiteralPath (Join-Path $OutputDirectory $name)) {
        throw "Output already exists: $name"
    }
}
$engine = New-Object -ComObject DAO.DBEngine.36
try {
    foreach ($entry in @(@('V1997',32),@('V2000',64))) {
        $path = Join-Path $OutputDirectory ('NativeQueryDependencies' + $entry[0] + '.mdb')
        $path = [System.IO.Path]::GetFullPath($path)
        $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0', $entry[1])
        try {
            $db.Execute('CREATE TABLE [T] ([Id] LONG, [Note] LONG, [Keep] LONG)', 128)
            $db.Execute('INSERT INTO [T] ([Id],[Note],[Keep]) VALUES (1,2,9)', 128)
            $db.Execute('CREATE TABLE [Lookup] ([Direct] LONG, [Sql] LONG, [Saved] LONG, [Chain] LONG, [Parameters] LONG, [Union] LONG, [CrossTab] LONG)', 128)
            $db.Execute('INSERT INTO [Lookup] ([Direct],[Sql],[Saved],[Chain],[Parameters],[Union],[CrossTab]) VALUES (1,1,1,1,1,1,1)', 128)
            $definitions = [ordered]@{
                BaseLookup = 'SELECT X.Id, X.Note FROM T AS X'
                SavedLookup = 'SELECT B.Id, B.Note FROM BaseLookup AS B'
                ParameterLookup = 'PARAMETERS [Minimum] Long; SELECT T.Id, T.Note FROM T WHERE T.Note > [Minimum]'
                UnionLookup = 'SELECT Id, Note FROM T UNION ALL SELECT Id, Note FROM T'
                CrossTabLookup = 'TRANSFORM Count(T.Id) AS Total SELECT T.Keep FROM T GROUP BY T.Keep PIVOT T.Note'
                UpdateDefinition = 'UPDATE [T] SET [Note]=[Note]+1 WHERE [Id]=1'
                AppendDefinition = 'INSERT INTO [T] ([Id],[Note],[Keep]) SELECT [Id]+100,[Note],[Keep] FROM [T]'
                DeleteDefinition = 'DELETE FROM [T] WHERE [Note]<0'
                MakeTableDefinition = 'SELECT [Id],[Note] INTO [NeverCreated] FROM [T]'
            }
            foreach ($definition in $definitions.GetEnumerator()) {
                $query = $db.CreateQueryDef($definition.Key, $definition.Value)
                $query.Close()
            }
            $sources = @('T', 'SELECT X.Id, X.Note FROM T AS X', 'BaseLookup', 'SavedLookup', 'ParameterLookup', 'UnionLookup', 'CrossTabLookup')
            $names = @('Direct', 'Sql', 'Saved', 'Chain', 'Parameters', 'Union', 'CrossTab')
            for ($i = 0; $i -lt $names.Length; $i++) {
                $field = $db.TableDefs('Lookup').Fields($names[$i])
                $field.Properties.Append($field.CreateProperty('RowSourceType', 10, 'Table/Query'))
                $field.Properties.Append($field.CreateProperty('RowSource', 12, $sources[$i]))
            }
        } finally { $db.Close() }
        Get-FileHash -LiteralPath $path -Algorithm SHA256
    }
} finally { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }
