param([Parameter(Mandatory)][string]$OutputDirectory)
$ErrorActionPreference = 'Stop'
[void][IO.Directory]::CreateDirectory($OutputDirectory)
$source = Join-Path $OutputDirectory 'native-operators.accdb'
$compacted = Join-Path $OutputDirectory 'native-operators-compacted.accdb'
$engine = New-Object -ComObject DAO.DBEngine.120
try {
    $db = $engine.CreateDatabase($source, ';LANGID=0x0409;CP=1252;COUNTRY=0', 128)
    try {
        $table = $db.CreateTableDef('Unary')
        $table.Fields.Append($table.CreateField('Id', 4))
        $table.Fields.Append($table.CreateField('S', 10, 50))
        $db.TableDefs.Append($table)
        foreach ($definition in @(@('PlusDouble', 7, '+[S]'), @('MinusDouble', 7, '-[S]'), @('PlusBinary', 9, '+[S]'), @('MinusBinary', 9, '-[S]'), @('PlusText', 10, '+[S]'))) {
            $field = $table.CreateField($definition[0], [int]$definition[1])
            $field.Expression = $definition[2]
            $table.Fields.Append($field)
        }
        $values = @('"bad"', '"1E999"', '"1E100"', '"1.2345678901234567"', '"1"', 'Null', '"1,5"', '"-0"')
        for ($index = 0; $index -lt $values.Count; $index++) {
            $db.Execute(('INSERT INTO [Unary] ([Id],[S]) VALUES (' + ($index + 1) + ',' + $values[$index] + ')'), 128)
        }
        $table = $db.CreateTableDef('Membership')
        $table.Fields.Append($table.CreateField('Id', 4))
        $db.TableDefs.Append($table)
        $expressions = @('1 In (1,1/0)', '1 In (2,1/0)', 'Null In (1/0,1)', '1 In (1/0,1)', '1 In (1/0,2)', 'Null In (1,1/0)', '1 In (1,Null)', '1 In (2,Null)', 'Null In (1,2)', '1 In (1,Sqr(-1))', '1 In (Sqr(-1),1)', '(1/0) In (1,2)')
        for ($index = 0; $index -lt $expressions.Count; $index++) {
            $field = $table.CreateField('V' + $index, 7)
            $field.Expression = $expressions[$index]
            $table.Fields.Append($field)
        }
        $db.Execute('INSERT INTO [Membership] ([Id]) VALUES (1)', 128)
    } finally { $db.Close() }
    $engine.CompactDatabase($source, $compacted)
} finally { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine) }
Get-FileHash $source, $compacted -Algorithm SHA256
