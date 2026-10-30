namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks the native fixed-offset DECIMAL cache, reserved prefix and partial payloads.</summary>
public sealed class CalculatedNumericNativeLayoutTests
{
    /// <summary>Gets native ACE results for every partial payload width.</summary>
    public static TheoryData<int, string> NativeWidths { get; } = new()
    {
        { 4, "0.00" },
        { 5, "737869762948382064.64" },
        { 6, "142408864249037738475.52" },
        { 7, "24320925256541621232599.04" },
        { 8, "3119171023469992308480409.60" },
        { 9, "3119171023469992308480409.68" },
        { 10, "3119171023469992308480427.60" },
        { 11, "3119171023469992308484359.76" },
        { 12, "3119171023469992309323220.56" },
        { 13, "3119171023469992824719296.08" },
        { 14, "3119171023470113770998351.44" },
        { 15, "3119171023498261268669417.04" },
        { 16, "3119171029983444732082931.28" },
    };

    [Theory]
    [MemberData(nameof(NativeWidths))]
    public void PartialPayload_ZeroExtendsTrailingDecimalBytes(int length, string expected)
    {
        byte[] payload = NativePayload()[..length];
        decimal number = decimal.Parse(expected, CultureInfo.InvariantCulture);
        Assert.Equal(number, CalculatedColumnUtil.ReadPayloadTyped(payload, ColumnType.NumericType, strictNumeric: true));
        Assert.Equal(number, CalculatedColumnUtil.ReadPayloadTyped(payload, ColumnType.NumericType, strictNumeric: false));
        Assert.Equal(expected, CalculatedColumnUtil.ReadPayloadString(payload, ColumnType.NumericType, strictNumeric: true));
        Assert.Equal(expected, CalculatedColumnUtil.ReadPayloadString(payload, ColumnType.NumericType, strictNumeric: false));
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1)]
    [InlineData((short)2)]
    [InlineData((short)3)]
    [InlineData((short)6)]
    [InlineData((short)14)]
    [InlineData((short)16)]
    [InlineData((short)18)]
    [InlineData((short)-1)]
    public void ReservedPrefix_DoesNotChangeValueOrMantissaWidth(short prefix)
    {
        byte[] payload = NativePayload();
        BinaryPrimitives.WriteInt16LittleEndian(payload, prefix);
        Assert.Equal(3119171029983444732082931.28m, CalculatedColumnUtil.ReadPayloadTyped(payload, ColumnType.NumericType, strictNumeric: true));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_ReservedPrefixesAndPartialPayloadsRetainNativeMeaningThroughCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeNumericLayout");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            $db.Close()
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.ACE.OLEDB.12.0;Data Source=' + {{path}})
                $connection.Execute('CREATE TABLE T (Id LONG,N DECIMAL(28,2))') | Out-Null
            } finally {
                $connection.Close()
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null
            }
            $db = $engine.OpenDatabase({{path}})
            try {
                $tdf = $db.TableDefs('T')
                $field = $tdf.CreateField('R',20)
                $field.Expression = '[N]'
                $tdf.Fields.Append($field)
                $rs = $db.OpenRecordset('T',2)
                try {
                    $rs.AddNew()
                    $rs.Fields('Id').Value = 1
                    $rs.Fields('N').Value = [decimal]::new(0x05060708,0x090A0B0C,0x01020304,$false,2)
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        byte[] source = await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken);
        int offset = FindNativePayload(source);
        var cases = new List<(string Path, string Compacted, decimal Expected)>();
        foreach (short prefix in new short[] { 0, 1, 2, 3, 6, 14, 16, 18, -1 })
        {
            byte[] bytes = (byte[])source.Clone();
            BinaryPrimitives.WriteInt16LittleEndian(bytes.AsSpan(offset, 2), prefix);
            string candidate = session.CreateDatabasePath($"prefix{prefix}");
            await File.WriteAllBytesAsync(candidate, bytes, TestContext.Current.CancellationToken);
            cases.Add((candidate, session.CreateDatabasePath($"prefix{prefix}-compacted"), 3119171029983444732082931.28m));
        }

        foreach (TheoryDataRow<int, string> row in NativeWidths)
        {
            byte[] bytes = (byte[])source.Clone();
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(offset - 4, 4), row.Data.Item1);
            string candidate = session.CreateDatabasePath($"length{row.Data.Item1}");
            await File.WriteAllBytesAsync(candidate, bytes, TestContext.Current.CancellationToken);
            cases.Add((candidate, session.CreateDatabasePath($"length{row.Data.Item1}-compacted"), decimal.Parse(row.Data.Item2, CultureInfo.InvariantCulture)));
        }

        var nativeCases = new List<string>();
        foreach ((string candidate, string compacted, decimal expected) in cases)
        {
            nativeCases.Add("@(" + AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(candidate) + "," + AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(compacted) + "," + AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(expected.ToString(CultureInfo.InvariantCulture)) + ")");
        }

        string controls = string.Join(",", nativeCases);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            foreach ($case in @({{controls}})) {
                $expected = [decimal]::Parse($case[2],[Globalization.CultureInfo]::InvariantCulture)
                $db = $engine.OpenDatabase($case[0])
                try {
                    $rs = $db.OpenRecordset('T')
                    try { if ($rs.Fields('R').Value -ne $expected) { throw ('Native Numeric value changed: ' + $case[0]) } }
                    finally { $rs.Close() }
                } finally { $db.Close() }
                $engine.CompactDatabase($case[0],$case[1])
                $db = $engine.OpenDatabase($case[1])
                try {
                    $rs = $db.OpenRecordset('T')
                    try { if ($rs.Fields('R').Value -ne $expected) { throw ('Native Numeric compact value changed: ' + $case[0]) } }
                    finally { $rs.Close() }
                } finally { $db.Close() }
            }
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        foreach ((string candidate, string compacted, decimal expected) in cases)
        {
            await AssertNativeValueAsync(candidate, expected);
            await AssertNativeValueAsync(compacted, expected);
        }
    }

    private static int FindNativePayload(byte[] source)
    {
        byte[] expected = NativePayload();
        int found = -1;
        for (int index = 4; index <= source.Length - expected.Length; index++)
        {
            if (BinaryPrimitives.ReadInt32LittleEndian(source.AsSpan(index - 4, 4)) == 16
                && source.AsSpan(index + 2, 14).SequenceEqual(expected.AsSpan(2)))
            {
                Assert.Equal(-1, found);
                found = index;
            }
        }

        Assert.NotEqual(-1, found);
        return found;
    }

    private static async Task AssertNativeValueAsync(string path, decimal expected)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(expected, Assert.IsType<decimal>(row[2]));
            count++;
        }

        Assert.Equal(1, count);
    }

    private static byte[] NativePayload() => [0, 0, 2, 0, 4, 3, 2, 1, 8, 7, 6, 5, 12, 11, 10, 9];
}
