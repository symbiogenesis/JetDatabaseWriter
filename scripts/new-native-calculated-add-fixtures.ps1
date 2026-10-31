[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'
if ((Get-Culture).Name -ne 'en-US') { throw 'Run the capture under an en-US Windows user regional profile.' }
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($OutputDirectory)) { throw 'Preserve each independent native capture in a new directory.' }
[void][IO.Directory]::CreateDirectory($OutputDirectory)
$Fixture = Join-Path $PSScriptRoot '../tests/JetDatabaseWriter.Tests/Databases/CalculatedStale/native-before.accdb'

function New-BasicControls {
$ErrorActionPreference='Stop'
$engine=New-Object -ComObject DAO.DBEngine.120
try {
$path=Join-Path $OutputDirectory 'native-add.accdb'
$db=$engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
try {
foreach($item in @(@('Normal','[N]*2',4,$false),@('Error','1/[N]',7,$false),@('Required','[N]*2',4,$true),@('ExistingRequired','[N]',4,$true))) {
$name=$item[0]
$db.Execute('CREATE TABLE ['+$name+'] (Id LONG,N LONG)',128)
$db.Execute('INSERT INTO ['+$name+'] (Id,N) VALUES (1,2)',128)
$db.Execute('INSERT INTO ['+$name+'] (Id,N) VALUES (2,0)',128)
$db.Execute('INSERT INTO ['+$name+'] (Id,N) VALUES (3,Null)',128)
$db.TableDefs.Refresh()
$td=$db.TableDefs($name)
$field=$td.CreateField('C',[int]$item[2]);$field.Expression=$item[1];$field.Required=$item[3]
$td.Fields.Append($field)
Write-Output ('APPENDED='+$name)
$rs=$db.OpenRecordset('SELECT * FROM ['+$name+'] ORDER BY Id',2)
try {while(!$rs.EOF){$value=$rs.Fields('C').Value;$type=if($null -eq $value){'COMNULL'}elseif($value -is [DBNull]){'DBNull'}else{$value.GetType().FullName};Write-Output ($name+':'+$rs.Fields('Id').Value+':'+$type+':'+$value);$rs.MoveNext()}}finally{$rs.Close()}
}
}finally{$db.Close()}
$engine.CompactDatabase($path,(Join-Path $OutputDirectory 'native-add-compacted.accdb'))
}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
}

function New-ExtendedControls {
$ErrorActionPreference='Stop'
$engine=New-Object -ComObject DAO.DBEngine.120
try {
$path=Join-Path $OutputDirectory 'native-add-extended.accdb'
$db=$engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
try {
$cases=@(@('Dependent','[C]+1',4,$false,$null),@('ErrorText','1/[N]',10,$false,$null),@('ErrorMemo','1/[N]',12,$false,$null),@('FieldRule','[N]*2',4,$false,'>100'),@('ErrorRequired','1/[N]',7,$true,$null))
foreach($item in $cases) {
$name=$item[0];$db.Execute('CREATE TABLE ['+$name+'] (Id LONG,N LONG)',128)
$db.Execute('INSERT INTO ['+$name+'] (Id,N) VALUES (1,2)',128);$db.Execute('INSERT INTO ['+$name+'] (Id,N) VALUES (2,0)',128);$db.Execute('INSERT INTO ['+$name+'] (Id,N) VALUES (3,Null)',128)
$db.TableDefs.Refresh();$td=$db.TableDefs($name)
if($name -eq 'Dependent'){$first=$td.CreateField('C',4);$first.Expression='[N]*2';$td.Fields.Append($first)}
$field=if($item[2] -eq 10){$td.CreateField('Added',10,243)}else{$td.CreateField('Added',[int]$item[2])}
$field.Expression=$item[1];$field.Required=$item[3];if($null -ne $item[4]){$field.ValidationRule=$item[4]}
try{$td.Fields.Append($field);Write-Output ('APPENDED='+$name)}catch{Write-Output ('REFUSED='+$name+':'+(@($engine.Errors|ForEach-Object Number)-join ','));continue}
$rs=$db.OpenRecordset('SELECT * FROM ['+$name+'] ORDER BY Id',2)
try{while(!$rs.EOF){$value=$rs.Fields('Added').Value;$type=if($null -eq $value){'COMNULL'}elseif($value -is [DBNull]){'DBNull'}else{$value.GetType().FullName};Write-Output ($name+':'+$rs.Fields('Id').Value+':'+$type+':'+$value);$rs.MoveNext()}}finally{$rs.Close()}
}
$db.Execute('CREATE TABLE TableRule (Id LONG,N LONG)',128);$db.Execute('INSERT INTO TableRule (Id,N) VALUES (1,2)',128);$db.Execute('INSERT INTO TableRule (Id,N) VALUES (2,0)',128);$db.TableDefs.Refresh();$td=$db.TableDefs('TableRule');$td.ValidationRule='[N]>10';$field=$td.CreateField('Added',4);$field.Expression='[N]*2';try{$td.Fields.Append($field);Write-Output 'APPENDED=TableRule'}catch{Write-Output ('REFUSED=TableRule:'+(@($engine.Errors|ForEach-Object Number)-join ','))}
}finally{$db.Close()}
try{$engine.CompactDatabase($path,(Join-Path $OutputDirectory 'native-add-extended-compacted.accdb'));Write-Output 'COMPACT=OK'}catch{Write-Output ('COMPACT=REFUSED:'+(@($engine.Errors|ForEach-Object Number)-join ','))}
}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
}

function New-RuleControl {
$ErrorActionPreference='Stop'
$engine=New-Object -ComObject DAO.DBEngine.120
try {
$path=Join-Path $OutputDirectory 'native-add-rule.accdb';$db=$engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
try{$db.Execute('CREATE TABLE T (Id LONG,N LONG)',128);$db.Execute('INSERT INTO T (Id,N) VALUES (1,2)',128);$db.Execute('INSERT INTO T (Id,N) VALUES (2,0)',128);$db.TableDefs.Refresh();$db.TableDefs('T').ValidationRule='[N]>10'}finally{$db.Close()}
$db=$engine.OpenDatabase($path)
try{$td=$db.TableDefs('T');$field=$td.CreateField('Added',4);$field.Expression='[N]*2';try{$td.Fields.Append($field);Write-Output 'APPEND=OK'}catch{Write-Output ('APPEND=REFUSED:'+($_.Exception.Message));foreach($failure in $engine.Errors){Write-Output ($failure.Number.ToString()+':'+$failure.Description)}};Write-Output ('FIELDS='+$td.Fields.Count)}finally{$db.Close()}
}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
}

function New-StaleControls {
$ErrorActionPreference='Stop';$engine=New-Object -ComObject DAO.DBEngine.120
try{foreach($mode in @('plain','independent','dependent')){$path=Join-Path $OutputDirectory ('native-stale-'+$mode+'.accdb');Copy-Item -LiteralPath $Fixture -Destination $path;$db=$engine.OpenDatabase($path);try{$td=$db.TableDefs('Renamed');$field=$td.CreateField('Added',4);if($mode -eq 'independent'){$field.Expression='42'}elseif($mode -eq 'dependent'){$field.Expression='[V]+1'};$td.Fields.Append($field);Write-Output ('STALE_MODE='+$mode+':OK')}finally{$db.Close()}}}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
}

New-BasicControls
New-ExtendedControls
New-RuleControl
New-StaleControls
Get-FileHash (Join-Path $OutputDirectory '*.accdb') -Algorithm SHA256
