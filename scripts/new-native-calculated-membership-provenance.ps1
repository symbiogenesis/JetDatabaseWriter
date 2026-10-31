[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'
$directory = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($directory)) { throw 'Preserve every native capture in a new directory.' }
[void][IO.Directory]::CreateDirectory($directory)
$path = Join-Path $directory 'native-membership-provenance.accdb'
$engine = New-Object -ComObject DAO.DBEngine.120
$cases = @(
'1 In("1")',
'"1" In(1)',
'1 In([S])',
'[S] In(1)',
'1.0 In([S])',
'[S] In(1.0)',
'-1 In([NegS])',
'[NegS] In(-1)',
'IIf(True,1,2) In([S])',
'[S] In(IIf(True,1,2))',
'[CalcD] In("1")',
'"1" In([CalcD])',
'[CalcI] In("1")',
'"1" In([CalcI])',
'[CalcS] In(1)',
'1 In([CalcS])',
'[CalcIIf] In("1")',
'"1" In([CalcIIf])',
'[CalcI] In([CalcS])',
'1 In("1",[S])',
'1 In([S],"1")',
'1 In(IIf(True,[S],"1"))',
'1 In(IIf(False,[S],"1"))',
'IIf(True,[CalcI],1) In("1")',
'IIf(False,[CalcI],1) In("1")',
'2 In("1",[S])',
'1 In(IIf(True,[CalcS],"1"))',
'1 In(IIf(False,[CalcS],"1"))'
)
$cases += @(
'1.5 In("1.5")','1E100 In("1E+100")','-1 In("-1")','(-1) In("-1")','-(1) In("-1")','-((1)) In("-1")',
'+1 In("1")','(1+0) In("1")','Abs(1) In("1")','CDbl(1) In("1")','1 In(1&"")','1 In(+"1")',
'1 In(Choose(1,"1","2"))','IIf(True,1,CDbl(2)) In("1")','IIf(True,CDbl(1),2) In("1")',
'1 In(IIf(True,"1",UCase("2")))','1 In(IIf(True,UCase("1"),"2"))','-IIf(True,1,2) In("-1")',
'IIf(True,1,2) In(UCase("1"))','-CDbl(0) In("-0")','-CDbl(0) In("0")',
'IsNull(Null) In("True")','"True" In(IsNull(Null))','IsNull(Null) In("-1")','"-1" In(IsNull(Null))',
'IsEmpty(1) In("0")','IsEmpty(1) In("False")','(1 Like "1") In("-1")',
'IIf(True,IsNull(Null),False) In("-1")','IIf(False,IsNull(Null),True) In("-1")'
)
$cases += @('- 1 In("-1")','- (1) In("-1")','-( 1) In("-1")','+ 1 In("1")','1 In( "1")','--1 In("1")','-(-1) In("1")')
try {
    $db=$engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
    try {
        $db.Execute('CREATE TABLE T(Id LONG,S TEXT(20),NegS TEXT(20))',128)
        $table=$db.TableDefs('T')
        foreach($definition in @(@('CalcD',7,'1'),@('CalcI',4,'1'),@('CalcS',10,'"1"'),@('CalcIIf',4,'IIf(True,1,2)'))) {
            $field=$table.CreateField($definition[0],$definition[1]);$field.Expression=$definition[2];$table.Fields.Append($field)
        }
        for($index=0;$index-lt$cases.Count;$index++){
            $field=$table.CreateField('V'+$index,7);$field.Expression=$cases[$index];$table.Fields.Append($field)
        }
        $db.Execute('INSERT INTO T(Id,S,NegS) VALUES(1,''1'',''-1'')',128)
        $record=$db.OpenRecordset('T',2)
        try { for($index=0;$index-lt$cases.Count;$index++){[pscustomobject]@{Expression=$cases[$index];Value=$record.Fields('V'+$index).Value}} }
        finally {$record.Close()}
    } finally {$db.Close()}
    $engine.CompactDatabase($path,(Join-Path $directory 'native-membership-provenance-compacted.accdb'))
} finally {[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
Get-FileHash $path, (Join-Path $directory 'native-membership-provenance-compacted.accdb') -Algorithm SHA256
