param([Parameter(Mandatory)][string]$OutputDirectory)
$ErrorActionPreference = 'Stop'
[void][IO.Directory]::CreateDirectory($OutputDirectory)
$source = Join-Path $OutputDirectory 'native-membership.accdb'
$compacted = Join-Path $OutputDirectory 'native-membership-compacted.accdb'
$cases = @(
    @('NumberText', '1 In ("1")'),
    @('NumberBadText', '1 In ("bad",1)'),
    @('TextNumber', '"1" In (1)'),
    @('TextBadNumber', '"bad" In (1)'),
    @('NumberGroupedText', '1 In ("1,0")'),
    @('NumberExponentText', '1 In ("1E999")'),
    @('BooleanText', 'True In ("bad")'),
    @('EarlyNumber', '1 In (1,"bad")'),
    @('EarlyText', '"1" In ("1",1)'),
    @('NullNeedle', 'Null In ("bad",1)'),
    @('NullCandidate', '1 In (Null,"1")'),
    @('DateCanonical', '#2020-01-01# In ("1/1/2020")'),
    @('DatePadded', '#2020-01-01# In ("01/01/2020")'),
    @('DateBadText', '#2020-01-01# In ("bad")'),
    @('DateAfterBadText', '#2020-01-01# In ("bad",#2020-01-01#)'),
    @('TextDate', '"1/1/2020" In (#2020-01-01#)'),
    @('BadTextDate', '"bad" In (#2020-01-01#)'),
    @('DateTimeCanonical', '#2020-01-01 12:00:00# In ("1/1/2020 12:00:00 PM")'),
    @('DateMidnightText', '#2020-01-01# In ("1/1/2020 12:00:00 AM")'),
    @('DateNumber', '#2020-01-01# In (43831)'),
    @('NumberDate', '43831 In (#2020-01-01#)'),
    @('GuidText', '{guid {00112233-4455-6677-8899-AABBCCDDEEFF}} In ("{00112233-4455-6677-8899-AABBCCDDEEFF}")'),
    @('TextGuid', '"{00112233-4455-6677-8899-AABBCCDDEEFF}" In ({guid {00112233-4455-6677-8899-AABBCCDDEEFF}})'),
    @('GuidNumber', '{guid {00112233-4455-6677-8899-AABBCCDDEEFF}} In (1)'),
    @('NumberGuid', '1 In ({guid {00112233-4455-6677-8899-AABBCCDDEEFF}})'),
    @('GuidBadText', '{guid {00112233-4455-6677-8899-AABBCCDDEEFF}} In ("bad")'),
    @('BadTextGuid', '"bad" In ({guid {00112233-4455-6677-8899-AABBCCDDEEFF}})'),
    @('InfinityText', 'SYD(1E308,0,1000,1) In ("inf")'),
    @('TextInfinity', '"inf" In (SYD(1E308,0,1000,1))'),
    @('InfinityBadText', 'SYD(1E308,0,1000,1) In ("bad")'),
    @('NegativeInfinityText', 'PV(-1,10,-100,1000) In ("-inf")'),
    @('NegativeInfinityOther', 'PV(-1,10,-100,1000) In ("-Infinity")'),
    @('NaNCanonical', 'PMT(-1,10,1000,0,1) In ("-nan(ind)")'),
    @('NaNOtherText', 'PMT(-1,10,1000,0,1) In ("nan")'),
    @('NaNNumber', 'PMT(-1,10,1000,0,1) In (1)'),
    @('NumberNaN', '1 In (PMT(-1,10,1000,0,1))'),
    @('InfinityNumber', 'PV(-1,10,-100,1000) In (0)'),
    @('InfinitySelf', 'PV(-1,10,-100,1000) In (PV(-1,10,-100,1000))')
)
$engine = New-Object -ComObject DAO.DBEngine.120
try {
    $db = $engine.CreateDatabase($source, ';LANGID=0x0409;CP=1252;COUNTRY=0', 128)
    try {
        $table = $db.CreateTableDef('Membership')
        $table.Fields.Append($table.CreateField('Id', 4))
        $db.TableDefs.Append($table)
        foreach ($case in $cases) {
            $field = $table.CreateField($case[0], 7)
            $field.Expression = $case[1]
            $table.Fields.Append($field)
        }
        $db.Execute('INSERT INTO [Membership] ([Id]) VALUES (1)', 128)
    } finally { $db.Close() }
    $engine.CompactDatabase($source, $compacted)
} finally { [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($engine) }
Get-FileHash $source, $compacted -Algorithm SHA256
