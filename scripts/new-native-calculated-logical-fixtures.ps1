param([Parameter(Mandatory)][string]$OutputDirectory)
$ErrorActionPreference = 'Stop'
[void][IO.Directory]::CreateDirectory($OutputDirectory)
$inputs = Join-Path $PSScriptRoot '../tests/JetDatabaseWriter.Tests/Databases/NativeCalculatedLogic'
$engine = New-Object -ComObject DAO.DBEngine.120
try {
    foreach ($name in @('logical', 'boolean')) {
        $source = Join-Path $OutputDirectory ($name + '.accdb')
        $compacted = Join-Path $OutputDirectory ($name + '-compacted.accdb')
        if ((Test-Path -LiteralPath $source) -or (Test-Path -LiteralPath $compacted)) { throw 'Native fixture destination already exists.' }
        $cases = @(Get-Content -LiteralPath (Join-Path $inputs ($name + '-inputs.json')) -Raw | ConvertFrom-Json)
        $database = $engine.CreateDatabase($source, ';LANGID=0x0409;CP=1252;COUNTRY=0')
        try {
            for ($index = 0; $index -lt $cases.Count; $index++) {
                $case = $cases[$index]
                $tableName = 'T' + $index
                $database.Execute('CREATE TABLE [' + $tableName + '] ([Id] LONG)', 128)
                $database.TableDefs.Refresh()
                $field = $database.TableDefs($tableName).CreateField('Value', [int]$case.ResultType)
                $field.Expression = $case.Expression
                try { $database.TableDefs($tableName).Fields.Append($field) }
                catch { Write-Output ($tableName + ' DECLARATION_REFUSED ' + $_.Exception.Message); continue }
                $database.Execute('INSERT INTO [' + $tableName + '] ([Id]) VALUES (1)', 128)
                $record = $database.OpenRecordset('SELECT [Value] FROM [' + $tableName + ']')
                try {
                    $value = $record.Fields(0).Value
                    $type = if ($null -eq $value) { 'COMNULL' } else { $value.GetType().FullName }
                    Write-Output ($name + '/' + $tableName + ' ' + $case.Expression + '=' + $type + ':' + [string]$value)
                } finally { $record.Close() }
            }
        } finally { $database.Close() }
        $engine.CompactDatabase($source, $compacted)
        Get-FileHash -LiteralPath $source, $compacted -Algorithm SHA256
    }
} finally { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine) }
