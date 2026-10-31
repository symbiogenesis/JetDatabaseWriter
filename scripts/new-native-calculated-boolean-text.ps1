[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'
$directory = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($directory)) { throw 'Preserve each independent native capture.' }
if ([cultureinfo]::CurrentCulture.Name -ne 'en-US') { throw 'These controls require an en-US user profile.' }
[void][IO.Directory]::CreateDirectory($directory)
$path = Join-Path $directory 'native-text.accdb'
$definitions = @(
    @('IsNull(Null) Like "True"', 7),
    @('IsNull(Null) Like "-1"', 7),
    @('Left(IsNull(Null),1)', 10),
    @('Len(IsNull(Null))', 7),
    @('InStr(1,IsNull(Null),"T")', 7),
    @('InStr(1,IsNull(Null),"-")', 7),
    @('Right(IsEmpty(1),1)', 10),
    @('UCase(IsNull(Null))', 10),
    @('Asc(IsNull(Null))', 7),
    @('Str(IsNull(Null))', 10),
    @('Str(IsEmpty(1))', 10),
    @('Str(1 Like "1")', 10),
    @('Asc(IsEmpty(1))', 7),
    @('Str(True)', 10),
    @('Str(False)', 10),
    @('Asc(True)', 7),
    @('Asc([Flag])', 7),
    @('Str([Flag])', 10),
    @('Asc([CalcFlag])', 7),
    @('Str([CalcFlag])', 10),
    @('Str(IIf(True,IsNull(Null),True))', 10),
    @('Str(IIf(False,IsNull(Null),True))', 10),
    @('Asc(IIf(True,IsNull(Null),True))', 7),
    @('Asc(IIf(False,IsNull(Null),True))', 7),
    @('Str$(IsNull(Null))', 10),
    @('Str$(IsEmpty(1))', 10),
    @('Asc(False)', 7)
)
$engine = New-Object -ComObject DAO.DBEngine.120
try {
    $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0', 128)
    try {
        $db.Execute('CREATE TABLE T(Id LONG,Flag YESNO)', 128)
        $table = $db.TableDefs('T')
        $field = $table.CreateField('CalcFlag', 1)
        $field.Expression = 'IsNull(Null)'
        $table.Fields.Append($field)
        for ($index = 0; $index -lt $definitions.Count; $index++) {
            $field = $table.CreateField('V' + $index, $definitions[$index][1])
            $field.Expression = $definitions[$index][0]
            $table.Fields.Append($field)
        }
        $db.Execute('INSERT INTO T(Id,Flag) VALUES(1,True)', 128)
        $record = $db.OpenRecordset('T', 2)
        try {
            for ($index = 0; $index -lt $definitions.Count; $index++) {
                [pscustomobject]@{ Expression = $definitions[$index][0]; Value = $record.Fields('V' + $index).Value } | ConvertTo-Json -Compress
            }
        } finally { $record.Close() }
    } finally { $db.Close() }
    $engine.CompactDatabase($path, (Join-Path $directory 'native-text-compacted.accdb'))
    Get-FileHash (Join-Path $directory '*.accdb') -Algorithm SHA256
} finally { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine) }
