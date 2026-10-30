[CmdletBinding()]
param([Parameter(Mandatory)][string] $OutputDirectory)
$ErrorActionPreference = 'Stop'
if ($env:NATIVE_CULTURE -and (Get-Culture).Name -ne $env:NATIVE_CULTURE) { throw 'The fresh process did not acquire the requested user regional profile.' }
$output = [IO.Path]::GetFullPath($OutputDirectory)
if ([IO.Directory]::Exists($output)) { throw 'Preserve each independent native profile run in a new directory.' }
[void][IO.Directory]::CreateDirectory($output)
$path = Join-Path $output 'native-profile.accdb'
$cases = @(
    @('DateText', '[D]&""', 10),
    @('DateAdd', '[D]+0.5', 8),
    @('Year', 'Year([D])', 7),
    @('MonthName', 'MonthName(3)', 10),
    @('MonthAbbrev', 'MonthName(9,True)', 10),
    @('WeekdayName', 'WeekdayName(2)', 10),
    @('WeekdayAbbrev', 'WeekdayName(2,True)', 10),
    @('WeekdaySystemName', 'WeekdayName(1,False,0)', 10),
    @('WeekdaySystem', 'Weekday([D],0)', 7),
    @('NumberText', '[N]&""', 10),
    @('NumberStr', 'Str([N])', 10),
    @('DotNumber', 'CDbl("1.5")', 7),
    @('CommaNumber', 'CDbl("1,5")', 7),
    @('DateParseMonth', 'Month("03/04/2020")', 7),
    @('DateParseDay', 'Day("03/04/2020")', 7),
    @('DateParseDMY', 'Day("31/12/2020")', 7),
    @('Upper', 'UCase([S])', 10),
    @('Lower', 'LCase([S])', 10),
    @('AnsiAsc', 'Asc("é")', 7),
    @('AnsiAscTurkish', 'Asc("İ")', 7),
    @('AnsiString', 'String(1,128)', 10),
    @('TwoDigitYear', 'DateSerial([Id]+48,1,1)', 8)
)
$engine = New-Object -ComObject DAO.DBEngine.120
$observations = [Collections.Generic.List[object]]::new()
try {
    $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0')
    try {
        $db.Execute('CREATE TABLE T (Id LONG,N DOUBLE,D DATETIME,S MEMO)', 128)
        $db.TableDefs.Refresh()
        $table = $db.TableDefs('T')
        foreach ($case in $cases) {
            $field = $table.CreateField($case[0], $case[2])
            $field.Expression = $case[1]
            $table.Fields.Append($field)
        }
        $record = $db.OpenRecordset('T', 2)
        try {
            $record.AddNew()
            $record.Fields('Id').Value = 1
            $record.Fields('N').Value = 1234.5
            $record.Fields('D').Value = [datetime]::new(2020, 3, 4, 13, 45, 0)
            $record.Fields('S').Value = 'iIıİäÄéÉßœ漢'
            $record.Update()
            $record.MoveFirst()
            foreach ($case in $cases) {
                $value = $record.Fields($case[0]).Value
                $type = if ($null -eq $value) { 'COMNULL' } elseif ($value -is [DBNull]) { 'DBNull' } else { $value.GetType().FullName }
                $rendered = if ($value -is [datetime]) { $value.ToString('O', [cultureinfo]::InvariantCulture) } elseif ($value -is [IFormattable]) { $value.ToString($null, [cultureinfo]::InvariantCulture) } else { [string]$value }
                $observations.Add([ordered]@{ Name = $case[0]; Expression = $case[1]; ResultType = $case[2]; Type = $type; Value = $rendered })
            }
        } finally { $record.Close() }
    } finally { $db.Close() }
    $engine.CompactDatabase($path, (Join-Path $output 'native-profile-compacted.accdb'))
    $report = [ordered]@{
        Culture = [cultureinfo]::CurrentCulture.Name
        InstalledUICulture = [cultureinfo]::InstalledUICulture.Name
        UserLanguage = (Get-WinUserLanguageList | Format-List * | Out-String).Trim()
        Engine = [string]$engine.Version
        OfficeVersion = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Office\ClickToRun\Configuration').VersionToReport
        PowerShell = $PSVersionTable.PSVersion.ToString()
        Observations = $observations
    }
    [IO.File]::WriteAllText((Join-Path $output 'observations.json'), ($report | ConvertTo-Json -Depth 6), [Text.UTF8Encoding]::new($false))
    $observations | Format-Table Name, Type, Value -AutoSize
    Get-FileHash (Join-Path $output '*.accdb') -Algorithm SHA256
} finally { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine) }
