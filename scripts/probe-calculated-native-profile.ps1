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
$monthCases = [Collections.Generic.List[object]]::new()
for ($month = 1; $month -le 12; $month++) {
    $monthCases.Add(@("Full$month", "MonthName($month)", 10))
    $monthCases.Add(@("Abbreviated$month", "MonthName($month,True)", 10))
}
$weekCases = [Collections.Generic.List[object]]::new()
for ($day = 1; $day -le 7; $day++) {
    foreach ($mode in @('Default', 'System', 'Sunday', 'Monday')) {
        $suffix = switch ($mode) { 'Default' { '' } 'System' { ',0' } 'Sunday' { ',1' } 'Monday' { ',2' } }
        $weekCases.Add(@("Full$mode$day", "WeekdayName($day,False$suffix)", 10))
        $weekCases.Add(@("Abbreviated$mode$day", "WeekdayName($day,True$suffix)", 10))
    }
}
$weekCases.Add(@('WeekdayDefault', 'Weekday([D])', 7))
$weekCases.Add(@('WeekdaySystem', 'Weekday([D],0)', 7))
$weekCases.Add(@('WeekdaySunday', 'Weekday([D],1)', 7))
$weekCases.Add(@('WeekdayMonday', 'Weekday([D],2)', 7))
$numberCases = [Collections.Generic.List[object]]::new()
$numberTexts = @('1.5', '1,5', '1,234.5', '1.234,5', '1 234,5', '1 234,5', '1 234,5', '1E3', '1,5E2', '1.5E2', '+1,5', '(1,5)', '$1,5', '€1,5')
for ($index = 0; $index -lt $numberTexts.Count; $index++) {
    $numberCases.Add(@("Parsed$index", ('CDbl("' + $numberTexts[$index] + '")'), 7))
    $numberCases.Add(@("Arithmetic$index", ('"' + $numberTexts[$index] + '"+0'), 7))
}
foreach ($definition in @(
    @('NegativeNumberText', '-[N]&""', 10),
    @('FractionText', '(1/3)&""', 10),
    @('ScientificText', 'CDbl(1E100)&""', 10),
    @('NegativeZeroText', '-CDbl(0)&""', 10),
    @('StrNegativeZero', 'Str("-0")', 10),
    @('StrLargeText', 'Str("1E100")', 10),
    @('StrPrecisionText', 'Str("12345678901234567")', 10),
    @('StrCommaText', 'Str("1,5")', 10),
    @('IntrinsicTrueText', 'IsNull(Null)&""', 10),
    @('IntrinsicFalseText', 'IsEmpty(1)&""', 10),
    @('LikeTrueText', '(1 Like "1")&""', 10),
    @('ComparisonTrueText', '(1=1)&""', 10),
    @('ConditionalBooleanText', 'IIf(True,IsNull(Null),False)&""', 10)
)) { $numberCases.Add($definition) }
foreach ($nonfinite in @(
    @('NegativeInfinity', 'PV(-1,10,-100,1000)'),
    @('Indefinite', 'PMT(-1,10,1000,0,1)'),
    @('PositiveInfinity', 'SYD(1E308,0,1000,1)')
)) {
    $numberCases.Add(@(($nonfinite[0] + 'Direct'), $nonfinite[1], 10))
    $numberCases.Add(@(($nonfinite[0] + 'Concat'), ('(' + $nonfinite[1] + ')&""'), 10))
    $numberCases.Add(@(($nonfinite[0] + 'Str'), ('Str(' + $nonfinite[1] + ')'), 10))
}
$dateCases = [Collections.Generic.List[object]]::new()
foreach ($definition in @(
    @('DateOnly', 'DateSerial(2020,3,4)&""', 10),
    @('Morning', '(DateSerial(2020,3,4)+TimeSerial(1,2,3))&""', 10),
    @('Noon', '(DateSerial(2020,3,4)+TimeSerial(12,0,0))&""', 10),
    @('Evening', '(DateSerial(2020,11,12)+TimeSerial(23,59,59))&""', 10),
    @('DayZero', 'DateSerial(1899,12,30)&""', 10),
    @('TimeOnly', 'TimeSerial(1,2,3)&""', 10),
    @('NegativeDate', '(DateSerial(1899,12,29)-TimeSerial(1,2,3))&""', 10),
    @('EarlyYear', 'DateSerial([Id]+99,3,4)&""', 10),
    @('LiteralMonth', 'Month(#03/04/2020#)', 7),
    @('LiteralDay', 'Day(#03/04/2020#)', 7)
)) { $dateCases.Add($definition) }
$dateTexts = @('03/04/2020', '03.04.2020', '03-04-2020', '2020/03/04', '2020-03-04', '31/12/2020', '01/02/00', '01/02/29', '01/02/30', '01/02/49', '01/02/50', '01/02/99')
for ($index = 0; $index -lt $dateTexts.Count; $index++) {
    foreach ($component in @('Year', 'Month', 'Day')) {
        $dateCases.Add(@("$component$index", ($component + '("' + $dateTexts[$index] + '")'), 7))
    }
}
$tables = [ordered]@{ T = $cases; MonthNames = $monthCases; WeekNames = $weekCases; RegionalNumbers = $numberCases; RegionalDates = $dateCases }
foreach ($firstByte in @(0, 128)) {
    $ansiCases = [Collections.Generic.List[object]]::new()
    for ($code = $firstByte; $code -lt $firstByte + 128; $code++) {
        $ansiCases.Add(@("Byte$code", "String(1,$code)", 10))
    }
    $tables["AnsiStrings$firstByte"] = $ansiCases
}
$tables['RegionalCase'] = @(
    @('WideAsc', 'Asc("漢")', 7),
    @('SupplementaryAsc', 'Asc("😀")', 7),
    @('BestFitAsc', 'Asc("Ā")', 7),
    @('DotlessAsc', 'Asc("ı")', 7),
    @('Upper', 'UCase("σςßſıİ𐐨")', 10),
    @('Lower', 'LCase("ΣΣẞSıİ𐐀")', 10)
)
$tables['RegionalGrammar'] = @(
    @('PunctuatedMonthDay', 'Day("Jan. 4, 2020")', 7),
    @('PunctuatedMonthYear', 'Year("4 Sept. 2020")', 7),
    @('NumericDotDate', 'Day("03.04.2020")', 7),
    @('JapaneseMonthOrder', 'Day("4 3月 2020")', 7),
    @('JapaneseUnitDate', 'Day("2020年 3月4日")', 7),
    @('LogicalCommaHalf', '"0,5" And True', 7),
    @('LogicalDotHalf', '"0.5" And True', 7),
    @('LogicalCurrencyHalf', '"$0.5" And True', 7),
    @('LogicalParenthesizedCurrencyHalf', '"($0.5)" And True', 7),
    @('DateNumericText', '#12:00:00# = "0.5"', 7),
    @('DateCurrencyText', '#12:00:00# = "$0.5"', 7),
    @('DateCommaText', '#12:00:00# = "0,5"', 7),
    @('DateTimeText', '#12:00:00# = "12:00:00"', 7),
    @('DateInvalidText', '#12:00:00# = "bad"', 7),
    @('DateFormattedText', '#12:00:00# = "12:00:00 PM"', 7),
    @('DateFullText', '#1/4/1900# = "1/4/1900"', 7),
    @('DateTextOrder', '#12:00:00# < "bad"', 7),
    @('DateSerialText', '#1/4/1900# = "5"', 7),
    @('DateSerialNumber', '#1/4/1900# = 5', 7),
    @('DateFractionNumber', '#12:00:00# = 0.5', 7)
)
$tables['RegionalBooleanResults'] = @(
    @('EnglishTrue', '"True"', 1),
    @('EnglishFalse', '"False"', 1),
    @('GermanTrue', '"Wahr"', 1),
    @('GermanFalse', '"Falsch"', 1),
    @('FrenchTrue', '"Vrai"', 1),
    @('FrenchFalse', '"Faux"', 1),
    @('IntrinsicTrueText', 'IsNull(Null)&""', 1),
    @('NumericDotHalf', '"0.5"', 1),
    @('NumericCommaHalf', '"0,5"', 1),
    @('NumericOne', '"1"', 1),
    @('InvalidText', '"bad"', 1)
)
$engine = New-Object -ComObject DAO.DBEngine.120
$observations = $null
$extended = [Collections.Generic.List[object]]::new()
try {
    $db = $engine.CreateDatabase($path, ';LANGID=0x0409;CP=1252;COUNTRY=0')
    try {
        foreach ($entry in $tables.GetEnumerator()) {
            $tableName = $entry.Key
            $definitions = $entry.Value
            if ($tableName -eq 'RegionalDates') {
                $nativeMarch = ($observations | Where-Object { $_.Name -eq 'MonthName' }).Value
                $nativeSeptember = ($observations | Where-Object { $_.Name -eq 'MonthAbbrev' }).Value
                $localizedDates = @("4 $nativeMarch 2020", "4 $nativeSeptember 2020")
                for ($index = 0; $index -lt $localizedDates.Count; $index++) {
                    foreach ($component in @('Year', 'Month', 'Day')) {
                        $quotedDate = $localizedDates[$index].Replace('"', '""')
                        $definitions.Add(@("Localized$component$index", ($component + '("' + $quotedDate + '")'), 7))
                    }
                }
            }
            $db.Execute("CREATE TABLE [$tableName] (Id LONG,N DOUBLE,D DATETIME,S MEMO)", 128)
            $db.TableDefs.Refresh()
            $table = $db.TableDefs($tableName)
            foreach ($case in $definitions) {
                try {
                    $field = $table.CreateField($case[0], $case[2])
                    $field.Expression = $case[1]
                    $table.Fields.Append($field)
                } catch { throw "Native declaration $tableName.$($case[0]) = $($case[1]): $($_.Exception.Message)" }
            }
            $record = $db.OpenRecordset($tableName, 2)
            try {
                $record.AddNew()
                $record.Fields('Id').Value = 1
                $record.Fields('N').Value = 1234.5
                $record.Fields('D').Value = [datetime]::new(2020, 3, 4, 13, 45, 0)
                $record.Fields('S').Value = 'iIıİäÄéÉßœ漢'
                $record.Update()
                $record.MoveFirst()
                $tableObservations = [Collections.Generic.List[object]]::new()
                foreach ($case in $definitions) {
                    $value = $record.Fields($case[0]).Value
                    $type = if ($null -eq $value) { 'COMNULL' } elseif ($value -is [DBNull]) { 'DBNull' } else { $value.GetType().FullName }
                    $rendered = if ($value -is [datetime]) { $value.ToString('O', [cultureinfo]::InvariantCulture) } elseif ($value -is [IFormattable]) { $value.ToString($null, [cultureinfo]::InvariantCulture) } else { [string]$value }
                    $tableObservations.Add([ordered]@{ Name = $case[0]; Expression = $case[1]; ResultType = $case[2]; Type = $type; Value = $rendered })
                }
                if ($tableName -eq 'T') { $observations = $tableObservations }
                else { $extended.Add([ordered]@{ Table = $tableName; Observations = $tableObservations }) }
                } finally { $record.Close() }
        }
    } finally { $db.Close() }
    $engine.CompactDatabase($path, (Join-Path $output 'native-profile-compacted.accdb'))
    $report = [ordered]@{
        Culture = [cultureinfo]::CurrentCulture.Name
        InstalledUICulture = [cultureinfo]::InstalledUICulture.Name
        UserLanguage = (Get-WinUserLanguageList | Format-List * | Out-String).Trim()
        Engine = [string]$engine.Version
        OfficeVersion = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Office\ClickToRun\Configuration').VersionToReport
        PowerShell = $PSVersionTable.PSVersion.ToString()
        RuntimeCultureMetadata = [ordered]@{
            UseUserOverride = [cultureinfo]::CurrentCulture.UseUserOverride
            Calendar = [cultureinfo]::CurrentCulture.DateTimeFormat.Calendar.GetType().Name
            ShortDatePattern = [cultureinfo]::CurrentCulture.DateTimeFormat.ShortDatePattern
            LongTimePattern = [cultureinfo]::CurrentCulture.DateTimeFormat.LongTimePattern
            FirstDayOfWeek = [string][cultureinfo]::CurrentCulture.DateTimeFormat.FirstDayOfWeek
            NumberDecimalSeparator = [cultureinfo]::CurrentCulture.NumberFormat.NumberDecimalSeparator
            NumberGroupSeparator = [cultureinfo]::CurrentCulture.NumberFormat.NumberGroupSeparator
            AMDesignator = [cultureinfo]::CurrentCulture.DateTimeFormat.AMDesignator
            PMDesignator = [cultureinfo]::CurrentCulture.DateTimeFormat.PMDesignator
        }
        Observations = $observations
        ExtendedTables = $extended
    }
    [IO.File]::WriteAllText((Join-Path $output 'observations.json'), ($report | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))
    $observations | ForEach-Object { [pscustomobject]$_ } | Format-Table Name, Type, Value -AutoSize
    Get-FileHash (Join-Path $output '*.accdb') -Algorithm SHA256
} finally { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine) }
