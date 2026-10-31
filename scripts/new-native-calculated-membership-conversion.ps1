[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'
$directory = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($directory)) { throw 'Preserve every native capture in a new directory.' }
if ([cultureinfo]::CurrentCulture.Name -ne 'en-US') { throw 'These native controls require an en-US user regional profile.' }
[void][IO.Directory]::CreateDirectory($directory)
$path = Join-Path $directory 'native-membership-conversion.accdb'
$cases=@(
'1 In([S])','[S] In(1)','CDbl(1) In([S])','[S] In(CDbl(1))','[D] In([S])','[S] In([D])',
'1 In([M])','[M] In(1)','CDbl(1) In([M])','[M] In(CDbl(1))','[D] In([M])','[M] In([D])',
'[I] In([S])','[S] In([I])','[I] In([M])','[M] In([I])','1.0 In([S])','1.0 In([M])'
)
$cases += @(
'1 In(UCase("bad"))','UCase("bad") In(1)','1 In(UCase("01"))','1 In(UCase("1.0"))','1 In(UCase("1.5"))',
'1 In(UCase("1E100"))','1 In(UCase("1E999"))','CDbl(1) In(UCase("bad"))','UCase("bad") In(CDbl(1))','UCase("01") In(CDbl(1))',
'9007199254740993 In(UCase("9007199254740993"))','9007199254740993 In(UCase("9007199254740992"))','9007199254740992 In(UCase("9007199254740993"))',
'UCase("9007199254740993") In(9007199254740993)','1.0000000000000002 In(UCase("1.0000000000000002"))',
'79228162514264337593543950335 In(UCase("79228162514264337593543950335"))','79228162514264337593543950335 In(UCase("1E100"))',
'1E100 In(UCase("1E100"))','1E100 In(UCase("bad"))','1 In(UCase("1.0000000000000000000000000001"))',
'1.5 In(UCase("1.5000000000000000000000000001"))','0 In(UCase("1E-100"))',
'IIf(True,1,CDbl(2)) In(UCase("bad"))','IIf(True,CDbl(1),2) In(UCase("bad"))','True In(UCase("True"))','True In(UCase("-1"))'
)
$engine=New-Object -ComObject DAO.DBEngine.120
try{
$db=$engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
try{
$db.Execute('CREATE TABLE T(Id LONG,S TEXT(255),M MEMO,D DOUBLE,I LONG)',128)
$table=$db.TableDefs('T')
$table.Fields('S').AllowZeroLength=$true
$table.Fields('M').AllowZeroLength=$true
for($index=0;$index-lt$cases.Count;$index++){$field=$table.CreateField('V'+$index,7);$field.Expression=$cases[$index];$table.Fields.Append($field)}
$id=0
foreach($text in @('1','01','1.0','bad','1E999','1E100','',$null,'iIıİäÄéÉßœ漢')){
$id++
$literal=if($null -eq $text){'Null'}else{"'"+$text.Replace("'","''")+"'"}
$db.Execute("INSERT INTO T(Id,S,M,D,I) VALUES($id,$literal,$literal,1,1)",128)
}
$record=$db.OpenRecordset('SELECT * FROM T ORDER BY Id',2)
try{
while(-not$record.EOF){for($index=0;$index-lt$cases.Count;$index++){[pscustomobject]@{Input=[string]$record.Fields('S').Value;Expression=$cases[$index];Value=$record.Fields('V'+$index).Value}|ConvertTo-Json -Compress};$record.MoveNext()}
}finally{$record.Close()}
}finally{$db.Close()}
$engine.CompactDatabase($path,(Join-Path $directory 'native-membership-conversion-compacted.accdb'))
}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
Get-FileHash $path,(Join-Path $directory 'native-membership-conversion-compacted.accdb') -Algorithm SHA256
