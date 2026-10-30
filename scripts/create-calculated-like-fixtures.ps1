[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory,
    [string]$Seed
)
$ErrorActionPreference='Stop'
$repositoryRoot = Split-Path $PSScriptRoot -Parent
$manifestPath = Join-Path $repositoryRoot 'tests/JetDatabaseWriter.Tests/Databases/CalculatedLike/cases.json'
$cases = @([IO.File]::ReadAllText($manifestPath) | ConvertFrom-Json | ForEach-Object { ,@($_.Source, $_.Pattern) })
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
[void][IO.Directory]::CreateDirectory($OutputDirectory)
$engine=New-Object -ComObject DAO.DBEngine.120
$name=if($Seed){'general'}else{'legacy'}
$path=Join-Path $OutputDirectory ("native-like-$name.accdb")
if(Test-Path -LiteralPath $path){throw 'Output already exists'}
if($Seed){[IO.File]::Copy((Resolve-Path $Seed).Path,$path);$db=$engine.OpenDatabase($path)}else{$db=$engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',128)}
function Literal($value) {if($null -eq $value){return 'Null'};return (([string]$value).Split([char]0)|ForEach-Object {"'"+$_.Replace("'","''")+"'"}) -join ' & Chr(0) & '}
$records=[Collections.Generic.List[object]]::new()
try {
 $db.Execute('CREATE TABLE [LikeCases]([Id] LONG,[S] LONGTEXT,[P] LONGTEXT)',128);$db.TableDefs.Refresh();$td=$db.TableDefs('LikeCases');$td.Fields('S').AllowZeroLength=$true;$td.Fields('P').AllowZeroLength=$true
 foreach($item in @(@('D',7,'[S] Like [P]'),@('B',1,'[S] Like [P]'),@('Masked',7,'IIf(True,42,[S] Like [P])'),@('AndFalse',1,'False And ([S] Like [P])'),@('OrTrue',1,'True Or ([S] Like [P])'))) {$f=$td.CreateField($item[0],$item[1]);$f.Expression=$item[2];$td.Fields.Append($f)}
 for($i=0;$i -lt $cases.Count;$i++) {
  $case=$cases[$i];$sql="INSERT INTO [LikeCases]([Id],[S],[P]) VALUES("+$i+","+(Literal $case[0])+","+(Literal $case[1])+")"
  $db.Execute($sql,128)
  $rs=$db.OpenRecordset('SELECT * FROM [LikeCases] WHERE Id='+$i)
  try{
   $v=$rs.Fields('D').Value;$kind=if($null -eq $v){'error'}elseif($v -is [DBNull]){'null'}elseif($v -eq 0){'false'}else{'true'}
   if($rs.Fields('Masked').Value -ne 42 -or $rs.Fields('AndFalse').Value -ne $false -or $rs.Fields('OrTrue').Value -ne $true){throw 'Masking control changed'}
   $records.Add([ordered]@{Id=$i;Source=$case[0];Pattern=$case[1];Expected=$kind})
  }finally{$rs.Close()}
 }
 $db.Execute('CREATE TABLE [Refused]([S] TEXT(255),[P] TEXT(255))',128);$db.TableDefs.Refresh();$td=$db.TableDefs('Refused');$f=$td.CreateField('V',10,243);$f.Expression='[S] Like [P]';$td.Fields.Append($f)
 $refused=$false
 try{$db.Execute("INSERT INTO [Refused]([S],[P]) VALUES('abc','[')",128)}catch{if(@($engine.Errors|ForEach-Object Number) -notcontains 3802){throw};$refused=$true}
 if(-not $refused){throw 'Text error conversion was accepted'}
}finally{$db.Close()}
[IO.File]::WriteAllText((Join-Path $OutputDirectory 'cases.json'),($records|ConvertTo-Json -Depth 5))
$engine.CompactDatabase($path,(Join-Path $OutputDirectory ("native-like-$name-compacted.accdb")))
Write-Output("PATH=$path; CASES="+$records.Count)
