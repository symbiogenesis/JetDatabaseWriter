param([Parameter(Mandatory)][string]$OutputDirectory)
$ErrorActionPreference='Stop'
[void][IO.Directory]::CreateDirectory($OutputDirectory)
$source=Join-Path $OutputDirectory 'native-date-serials.accdb'
$compacted=Join-Path $OutputDirectory 'native-date-serials-compacted.accdb'
if((Test-Path -LiteralPath $source) -or (Test-Path -LiteralPath $compacted)){throw 'Native fixture destination already exists.'}
$engine=New-Object -ComObject DAO.DBEngine.120
try {
    $db=$engine.CreateDatabase($source,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
    try {
        $db.Execute('CREATE TABLE [T]([Id] LONG,[At] DATETIME,[Label] TEXT(20))',128)
        $db.TableDefs.Refresh()
        $field=$db.TableDefs('T').CreateField('CalculatedAt',8)
        $field.Expression='TimeSerial(-1,0,0)'
        $db.TableDefs('T').Fields.Append($field)
        $db.Execute('INSERT INTO [T]([Id],[At],[Label]) VALUES(1,TimeSerial(-1,0,0),"negative")',128)
        $db.Execute('INSERT INTO [T]([Id],[At],[Label]) VALUES(2,#1:00 AM#,"positive")',128)
        $db.Execute('INSERT INTO [T]([Id],[At],[Label]) VALUES(3,#12:00 AM#,"zero")',128)
        $db.Execute('INSERT INTO [T]([Id],[At],[Label]) VALUES(4,#1:00 AM#+0.0000000001,"fraction")',128)
        $db.Execute('CREATE INDEX [AtIndex] ON [T]([At])',128)
        $rs=$db.OpenRecordset('SELECT [Id],CDbl([At]) AS [Serial],CDbl([CalculatedAt]) AS [CalculatedSerial] FROM [T] ORDER BY [Id]')
        try {while(!$rs.EOF){Write-Output ($rs.Fields('Id').Value.ToString()+'='+$rs.Fields('Serial').Value.ToString('R',[Globalization.CultureInfo]::InvariantCulture)+';'+$rs.Fields('CalculatedSerial').Value.ToString('R',[Globalization.CultureInfo]::InvariantCulture));$rs.MoveNext()}} finally {$rs.Close()}
    } finally {$db.Close()}
    $engine.CompactDatabase($source,$compacted)
} finally {[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
