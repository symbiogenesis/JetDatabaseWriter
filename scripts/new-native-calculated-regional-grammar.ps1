[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)

$ErrorActionPreference = 'Stop'
$directory = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($directory)) { throw 'Preserve every native capture in a new directory.' }
if ([cultureinfo]::CurrentCulture.Name -ne 'en-US') { throw 'These independent controls require an en-US user regional profile.' }
[void][IO.Directory]::CreateDirectory($directory)
$path = Join-Path $directory 'native-grammar.accdb'
$cases = @(
    'Day("Jan. 4, 2020")',
    'Year("4 Sept. 2020")',
    'Day("Jan 4, 2020")',
    'Month("4 September 2020")',
    'Day("03.04.2020")',
    'Day("2020-03-04 1:02:03 PM")',
    'Day("4 3月 2020")',
    'Day("2020年 3月4日")',
    '"0,5" And True',
    '"0.5" And True',
    '"$0.5" And True',
    '"($0.5)" And True',
    '#12:00:00# = "0.5"',
    '#12:00:00# = "$0.5"',
    '#12:00:00# = "0,5"',
    '#1/4/1900# = "0,5"',
    '#12:00:00# = "12:00:00"',
    '#12:00:00# = "bad"',
    '#12:00:00# = "12:00:00 PM"',
    '#1/4/1900# = "1/4/1900"',
    '#12:00:00# < "bad"',
    '#1/4/1900# = "5"',
    '#1/4/1900# = 5',
    '#12:00:00# = 0.5'
)
$engine = New-Object -ComObject DAO.DBEngine.120
try {
    $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0', 128)
    try {
        $db.Execute('CREATE TABLE T (Id LONG)', 128)
        $table = $db.TableDefs('T')
        for ($index = 0; $index -lt $cases.Count; $index++) {
            $field = $table.CreateField('V' + $index, 7)
            $field.Expression = $cases[$index]
            $table.Fields.Append($field)
        }
        $db.Execute('INSERT INTO T(Id) VALUES(1)', 128)
        $record = $db.OpenRecordset('T', 2)
        try {
            for ($index = 0; $index -lt $cases.Count; $index++) {
                [pscustomobject]@{ Expression = $cases[$index]; Value = $record.Fields('V' + $index).Value }
            }
        } finally { $record.Close() }
    } finally { $db.Close() }
    $engine.CompactDatabase($path, (Join-Path $directory 'native-grammar-compacted.accdb'))
    [pscustomobject]@{ Culture = [cultureinfo]::CurrentCulture.Name; Engine = $engine.Version }
    Get-FileHash (Join-Path $directory '*.accdb') -Algorithm SHA256
} finally { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine) }
