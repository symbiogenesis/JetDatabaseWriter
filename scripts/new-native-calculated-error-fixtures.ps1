param([Parameter(Mandatory)][string]$OutputDirectory)
$ErrorActionPreference = 'Stop'
[void][IO.Directory]::CreateDirectory($OutputDirectory)
$source = Join-Path $OutputDirectory 'native-calculated-errors.accdb'
$compacted = Join-Path $OutputDirectory 'native-calculated-errors-compacted.accdb'
$engine = New-Object -ComObject DAO.DBEngine.120
$types = @(@('Boolean',1),@('Byte',2),@('Integer',3),@('Long',4),@('Currency',5),@('Single',6),@('Double',7),@('DateTime',8),@('Text',10),@('Ole',11),@('Memo',12),@('Guid',15),@('Decimal',20),@('BigInt',16))
try {
    $db = $engine.CreateDatabase($source,';LANGID=0x0409;CP=1252;COUNTRY=0',128)
    try {
        foreach ($scenario in @(@('Overflow','[N]'),@('Mismatch','[S]'),@('Propagation','[Bad]'))) {
            $name = $scenario[0]
            $db.Execute(('CREATE TABLE ['+$name+'] ([Id] LONG,[N] DOUBLE,[S] TEXT(50))'),128)
            $db.TableDefs.Refresh()
            $table = $db.TableDefs($name)
            if ($name -eq 'Propagation') {
                $bad = $table.CreateField('Bad',2)
                $bad.Expression = '[N]'
                $table.Fields.Append($bad)
            }
            foreach ($type in $types) {
                if ($name -eq 'Propagation' -and $type[1] -in @(10,12)) {continue}
                $field = $table.CreateField($type[0],[int]$type[1])
                $field.Expression = $scenario[1]
                $table.Fields.Append($field)
            }
            if ($name -eq 'Propagation') {
                foreach ($expression in @(@('Dependent','[Bad]+1'),@('NullTest','IsNull([Bad])'),@('Length','Len([Bad])'),@('IgnoreLeft','IIf(False,[Bad],42)'),@('IgnoreRight','IIf(True,42,[Bad])'),@('AndFalse','False And [Bad]'),@('OrTrue','True Or [Bad]'))) {
                    $field = $table.CreateField($expression[0],4)
                    $field.Expression = $expression[1]
                    $table.Fields.Append($field)
                }
            }
            foreach ($row in @(@(1,'1e100','"bad"'),@(2,'Null','Null'),@(3,'1','"1"'),@(4,'300','"300"'))) {
                $db.Execute(('INSERT INTO ['+$name+'] ([Id],[N],[S]) VALUES ('+$row[0]+','+$row[1]+','+$row[2]+')'),128)
            }
            $rs = $db.OpenRecordset(('SELECT * FROM ['+$name+'] ORDER BY [Id]'))
            try {
                while (!$rs.EOF) {
                    foreach ($field in $rs.Fields) {
                        $value = $field.Value
                        $display = if ($null -eq $value) {'<error>'} elseif ($value -is [DBNull]) {'<null>'} else {$value.ToString()}
                        Write-Output ($name+'.'+$rs.Fields('Id').Value+'.'+$field.Name+'='+$display)
                    }
                    $rs.MoveNext()
                }
            } finally {$rs.Close()}
        }
        $db.Execute('CREATE TABLE [FunctionNull] ([N] DOUBLE)',128)
        $db.TableDefs.Refresh()
        $table = $db.TableDefs('FunctionNull')
        foreach ($name in @('Atn','Cos','Exp','Log','Sin','Sqr','Tan','CDbl')) {
            $field = $table.CreateField($name,7)
            $field.Expression = $name+'([N])'
            $table.Fields.Append($field)
        }
        $db.Execute('INSERT INTO [FunctionNull] ([N]) VALUES (Null)',128)
    } finally {$db.Close()}
    $engine.CompactDatabase($source,$compacted)
} finally {[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine)}
