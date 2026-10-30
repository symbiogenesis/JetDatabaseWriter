[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'
$output = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($output)) { throw 'Preserve independently authored controls in a new directory.' }
[void][IO.Directory]::CreateDirectory($output)
$path = Join-Path $output 'choose.accdb'
$compacted = Join-Path $output 'choose-compacted.accdb'
$engine = New-Object -ComObject DAO.DBEngine.120
$observations = [Collections.Generic.List[object]]::new()
$values = @('Null','0.5','0.9999999','0.99999999','1.5','1.9999999','1.99999999','2.5','2.99999999','3.5','4','2147483648','1E30','3.40282355E38','3.4028236E38','1E100','-1E30','-1E100')
try {
    $db = $engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
    try {
        $db.Execute('CREATE TABLE T (Id LONG,N DOUBLE,S TEXT(50))',128)
        $db.Execute('CREATE TABLE SingleIndices (Id LONG,N SINGLE)',128)
        $db.Execute('CREATE TABLE NonfiniteIndices (Id LONG)',128)
    } finally { $db.Close() }
    $connection = New-Object -ComObject ADODB.Connection
    try {
        $connection.Open('Provider=Microsoft.ACE.OLEDB.12.0;Data Source=' + $path)
        [void]$connection.Execute('CREATE TABLE DecimalIndices (Id INT,N DECIMAL(28,10))')
        $decimalValues = @('Null','0.99999999','1.9999999','1.99999999','2147483648','100000000000000000')
        for ($id=0; $id -lt $decimalValues.Count; $id++) {
            [void]$connection.Execute("INSERT INTO DecimalIndices (Id,N) VALUES ($id,$($decimalValues[$id]))")
        }
    } finally { $connection.Close(); [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($connection) }
    $db = $engine.OpenDatabase($path)
    try {
        $db.TableDefs.Refresh()
        foreach ($name in @('T','SingleIndices','DecimalIndices','NonfiniteIndices')) {
            $table = $db.TableDefs($name)
            if ($name -eq 'NonfiniteIndices') {
                $field = $table.CreateField('N',7)
                $field.Expression = 'IIf([Id]=0,PV(-1,10,-100,1000),PMT(-1,10,1000,0,1))'
                $table.Fields.Append($field)
            }
            foreach ($case in @(@('Choice',7,'Choose([N],10,20,30)'),@('ChoiceBinary',9,'Choose([N],10,20,30)'))) {
                $field = $table.CreateField($case[0],$case[1]); $field.Expression=$case[2]; $table.Fields.Append($field)
            }
            if ($name -eq 'T') {
                $field=$table.CreateField('ChoiceText',7); $field.Expression='Choose([S],10,20,30)'; $table.Fields.Append($field)
            }
            if ($name -eq 'NonfiniteIndices') {
                foreach ($case in @(@('DirectInfinity',7,'Choose(PV(-1,10,-100,1000),10,20,30)'),@('DirectNaN',7,'Choose(PMT(-1,10,1000,0,1),10,20,30)'))) {
                    $field=$table.CreateField($case[0],$case[1]); $field.Expression=$case[2]; $table.Fields.Append($field)
                }
            }
        }
        for ($id=0; $id -lt $values.Count; $id++) {
            $value=$values[$id]; $text=if($value-eq'Null'){'Null'}else{'"'+$value+'"'}
            $db.Execute("INSERT INTO T (Id,N,S) VALUES ($id,$value,$text)",128)
        }
        $singleValues=@('Null','0.99999999','1.9999999','1.99999999','16777217','3.4028235E38')
        for ($id=0; $id -lt $singleValues.Count; $id++) {
            $db.Execute("INSERT INTO SingleIndices (Id,N) VALUES ($id,$($singleValues[$id]))",128)
        }
        $db.Execute('INSERT INTO NonfiniteIndices (Id) VALUES (0)',128)
        $db.Execute('INSERT INTO NonfiniteIndices (Id) VALUES (1)',128)
        foreach ($name in @('T','SingleIndices','DecimalIndices','NonfiniteIndices')) {
            $record=$db.OpenRecordset("SELECT * FROM [$name] ORDER BY Id")
            try {
                while(!$record.EOF) {
                    $fields=[ordered]@{}
                    foreach($field in $record.Fields) {
                        $value=$field.Value
                        $type=if($null-eq$value){'COMNULL'}elseif($value-is[DBNull]){'DBNull'}else{$value.GetType().FullName}
                        $rendered=if($value-is[byte[]]){[Convert]::ToHexString($value)}elseif($value-is[IFormattable]){$value.ToString($null,[cultureinfo]::InvariantCulture)}else{[string]$value}
                        $fields[$field.Name]=[ordered]@{Type=$type;Value=$rendered}
                    }
                    $observations.Add([ordered]@{Table=$name;Id=[int]$record.Fields('Id').Value;Fields=$fields})
                    $record.MoveNext()
                }
            } finally {$record.Close()}
        }
    } finally {$db.Close()}
    $engine.CompactDatabase($path,$compacted)
    $report=[ordered]@{Culture=[cultureinfo]::CurrentCulture.Name;Engine=[string]$engine.Version;OfficeVersion=(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Office\ClickToRun\Configuration').VersionToReport;Is64BitProcess=[Environment]::Is64BitProcess;Inputs=$values;Observations=$observations;SourceHash=(Get-FileHash $path -Algorithm SHA256).Hash;CompactedHash=(Get-FileHash $compacted -Algorithm SHA256).Hash}
    [IO.File]::WriteAllText((Join-Path $output 'observations.json'),($report|ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))
    foreach($row in $observations) { Write-Output ("{0}.{1}: N={2}; Choice={3}/{4}; Binary={5}; DirectInfinity={6}; DirectNaN={7}" -f $row.Table,$row.Id,$row.Fields.N.Value,$row.Fields.Choice.Type,$row.Fields.Choice.Value,$row.Fields.ChoiceBinary.Value,$row.Fields.DirectInfinity.Type,$row.Fields.DirectNaN.Type) }
} finally {[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
