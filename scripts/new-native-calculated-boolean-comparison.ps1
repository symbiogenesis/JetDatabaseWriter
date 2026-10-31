[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'
$directory = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($directory)) { throw 'Preserve every independent native capture.' }
if ([cultureinfo]::CurrentCulture.Name -ne 'en-US') { throw 'These controls require an en-US user regional profile.' }
[void][IO.Directory]::CreateDirectory($directory)
$path = Join-Path $directory 'native-comparison.accdb'
$engine = New-Object -ComObject DAO.DBEngine.120
$cases = @(
'IsNull(Null)="True"',
'IsNull(Null)="-1"',
'"True"=IsNull(Null)',
'"-1"=IsNull(Null)',
'IsEmpty(1)="False"',
'IsEmpty(1)="0"',
'(1 Like "1")="True"',
'(1 Like "1")="-1"',
'IsNull(Null)<"0"',
'IsEmpty(1)>"-1"',
'IIf(True,IsNull(Null),False)="True"',
'IIf(True,IsNull(Null),False)="-1"',
'Choose(1,IsNull(Null),False)="-1"',
'[Flag]="-1"',
'[Flag]="True"',
'[CalcFlag]="-1"',
'[CalcFlag]="True"'
)
try {
    $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0', 128)
    try {
        $db.Execute('CREATE TABLE T(Id LONG,Flag YESNO)',128)
        $table = $db.TableDefs('T')
        $field=$table.CreateField('CalcFlag',1)
        $field.Expression='IsNull(Null)'
        $table.Fields.Append($field)
        for($index=0;$index-lt$cases.Count;$index++) {
            $field=$table.CreateField('V'+$index,7)
            $field.Expression=$cases[$index]
            $table.Fields.Append($field)
        }
        $db.Execute('INSERT INTO T(Id,Flag) VALUES(1,True)',128)
        $record=$db.OpenRecordset('T',2)
        try {for($index=0;$index-lt$cases.Count;$index++){[pscustomobject]@{Expression=$cases[$index];Value=$record.Fields('V'+$index).Value}}}
        finally{$record.Close()}
    } finally{$db.Close()}
    $engine.CompactDatabase($path,(Join-Path $directory 'native-comparison-compacted.accdb'))
} finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}