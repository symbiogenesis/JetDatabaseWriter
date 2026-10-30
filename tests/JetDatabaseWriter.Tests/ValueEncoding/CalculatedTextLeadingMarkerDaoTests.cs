namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native calculated Text conversion controls for leading compression markers.</summary>
/// <param name="output">Records the independent native conversion results.</param>
public sealed class CalculatedTextLeadingMarkerDaoTests(ITestOutputHelper output)
{
    private const string NativeReportFunction = """
        function Report([string]$path) {
            $db=$engine.OpenDatabase($path,$false,$true)
            try {
                $rs=$db.OpenRecordset('SELECT Id,Cached,CachedLength FROM Items ORDER BY Id',2)
                try {
                    while(!$rs.EOF) {
                        $cachedValue=$rs.Fields('Cached').Value
                        $lengthValue=$rs.Fields('CachedLength').Value
                        $cachedType=if($null -eq $cachedValue) { 'COMNULL' } else { $cachedValue.GetType().FullName }
                        $lengthType=if($null -eq $lengthValue) { 'COMNULL' } else { $lengthValue.GetType().FullName }
                        Write-Output ("NATIVE="+$rs.Fields('Id').Value+":"+[BitConverter]::ToString([Text.Encoding]::Unicode.GetBytes([string]$cachedValue)).Replace('-','')+":LENGTH="+$lengthValue+":TYPE="+$cachedType+":"+$lengthType)
                        $rs.MoveNext()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
        }
        """;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("\uFEFF", null, "FFFE")]
    [InlineData("\uFEFFbbb", "bbb", "FFFE620062006200")]
    [InlineData("\uFEFFbbbb", "\uFEFFbbbb", "FFFE00FFFE0062626262")]
    [InlineData("\uFEFF\u4E2Dtail", "\uFEFF\u4E2Dtail", "FFFE00FFFE2D4E007461696C")]
    [InlineData("\uFEFF\u0100tail", "\u7401ail", "FFFE00017400610069006C00")]
    [InlineData("\uFEFF\0tail", "tail", "FFFE00007400610069006C00")]
    [InlineData("\uFEFFtail\u4E2D", "tail\u4E2D", "FFFE7400610069006C002D4E")]
    public async Task CalculatedTextLeadingMarker_InsertAndUpdateMatchNativeConversion(string value, string? expected, string payloadHex)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Content", typeof(string), 255) { IsCompressedUnicode = false }, new("Cached", typeof(string), 243) { IsCalculated = true, CalculationExpression = "[Content]" }], Ct);
            await writer.InsertRowAsync("Items", [0, "original", null], Ct);
            await writer.InsertRowAsync("Items", [1, value, null], Ct);
            await writer.UpdateRowsAsync("Items", "Id", 0, new Dictionary<string, object?> { ["Content"] = value }, Ct);
        }

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.All(await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct), row => Assert.Equal((object?)expected ?? DBNull.Value, row[2]));
        Assert.All(await ReadPayloadsAsync(reader), payload => Assert.Equal(payloadHex, Convert.ToHexString(payload)));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoCalculatedTextLeadingMarker_WriterMatchesNativeReadUpdateAndCompact()
    {
        string[] values = Values();
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CalculatedTextMarkers");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(native.SourcePath, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Content", typeof(string), 255) { IsCompressedUnicode = false }, new("Cached", typeof(string), 243) { IsCalculated = true, CalculationExpression = "[Content]" }, new("CachedLength", typeof(int)) { IsCalculated = true, CalculationExpression = "Len([Cached])" }], Ct);
        }

        byte[] emptySchema = await File.ReadAllBytesAsync(native.SourcePath, Ct);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string encoded = string.Join(',', values.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(value)))));
        AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(
            $$"""
            {{NativeReportFunction}}
            $values=@({{encoded}})
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('Items',2)
                try {
                    for($id=0;$id -lt $values.Count;$id++) {
                        $rs.AddNew()
                        $rs.Fields.Item('Id').Value=$id
                        $rs.Fields.Item('Content').Value=[string][Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id]))
                        $rs.Update()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Report {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Report {{compacted}}
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        output.WriteLine(result.StdOut);
        (object Cached, object Length)[] expected = ParseNativeRows(result.StdOut, values.Length, repeats: 2);
        List<byte[]> nativePayloads;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(native.SourcePath, cancellationToken: Ct))
        {
            nativePayloads = await ReadPayloadsAsync(reader);
        }

        foreach (string path in new[] { native.SourcePath, native.CompactedPath })
        {
            await AssertRowsAsync(path, values, expected);
        }

        string subjectPath = Path.Combine(native.WorkDir, "writer.accdb");
        string subjectCompactedPath = Path.Combine(native.WorkDir, "writer-compacted.accdb");
        await File.WriteAllBytesAsync(subjectPath, emptySchema, Ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(subjectPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.InsertRowsAsync("Items", values.Select(static (value, id) => new object?[] { id, value, null, null }), Ct);
        }

        await AssertRowsAsync(subjectPath, values, expected);
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(subjectPath, cancellationToken: Ct))
        {
            Assert.Equal(
                nativePayloads.Select(static payload => Convert.ToHexString(payload)).Order(StringComparer.Ordinal),
                (await ReadPayloadsAsync(reader)).Select(static payload => Convert.ToHexString(payload)).Order(StringComparer.Ordinal));
        }

        string subject = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subjectPath);
        string subjectCompacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subjectCompactedPath);
        result = native.RunDaoEngineScript(NativeReportFunction + Environment.NewLine + "Report " + subject, TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        Assert.Equal(expected, ParseNativeRows(result.StdOut, values.Length, repeats: 1));

        await using (AccessWriter writer = await AccessWriter.OpenAsync(subjectPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.UpdateRowsAsync("Items", "Id", 0, new Dictionary<string, object?> { ["Content"] = values[15] }, Ct);
        }

        values[0] = values[15];
        expected[0] = expected[15];
        string replacement = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(values[23])));
        result = native.RunDaoEngineScript(
            $$"""
            {{NativeReportFunction}}
            $db=$engine.OpenDatabase({{subject}})
            try {
                $rs=$db.OpenRecordset('SELECT * FROM Items WHERE Id=1',2)
                try {
                    $rs.Edit()
                    $rs.Fields('Content').Value=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String({{replacement}}))
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Report {{subject}}
            $engine.CompactDatabase({{subject}},{{subjectCompacted}})
            Report {{subjectCompacted}}
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        values[1] = values[23];
        expected[1] = expected[23];
        Assert.Equal(expected, ParseNativeRows(result.StdOut, values.Length, repeats: 2));
        await AssertRowsAsync(subjectPath, values, expected);
        await AssertRowsAsync(subjectCompactedPath, values, expected);
    }

    private static (object Cached, object Length)[] ParseNativeRows(string report, int count, int repeats)
    {
        string[] lines = report.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(static line => line.Trim()).Where(static line => line.StartsWith("NATIVE=", StringComparison.Ordinal)).ToArray();
        Assert.Equal(count * repeats, lines.Length);
        for (int repeat = 1; repeat < repeats; repeat++)
        {
            Assert.Equal(lines.Take(count), lines.Skip(repeat * count).Take(count));
        }

        return lines.Take(count).Select(static line =>
        {
            string[] parts = line.Split(':');
            object cached = parts[3] == "TYPE=System.DBNull" ? DBNull.Value : Encoding.Unicode.GetString(Convert.FromHexString(parts[1]));
            object length = parts[4] == "System.DBNull" ? DBNull.Value : int.Parse(parts[2][7..], System.Globalization.CultureInfo.InvariantCulture);
            return (cached, length);
        }).ToArray();
    }

    private static async Task AssertRowsAsync(string path, string[] values, (object Cached, object Length)[] expected)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        object[][] rows = (await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct)).OrderBy(static row => (int)row[0]).ToArray();
        Assert.Equal(values.Length, rows.Length);
        for (int index = 0; index < values.Length; index++)
        {
            Assert.Equal(values[index], rows[index][1]);
            Assert.Equal(expected[index].Cached, rows[index][2]);
            Assert.Equal(expected[index].Length, rows[index][3]);
        }
    }

    private static string[] Values()
        => [.. Enumerable.Range(0, 9).Select(static length => "\uFEFF" + new string('b', length)), "\uFEFF\u4E2D", "\uFEFF\u03A9", "\uFEFF\u0100", "\uFEFF\u0200", "\uFEFF\0", "\uFEFF\0tail", "\uFEFF\u4E2Dtail", "\uFEFF\u0100tail", "\uFEFF\u4E2D\0tail", "\uFEFFtail\u4E2D", "\uFEFF\uD83D\uDE00", "\uFEFF\uD83D\uDE00tail", "\uFEFF\uFEFF", "\uFEFF\uFEFFtail", "\uFEFFbbbbbbbb\u4E2D", "\uFEFFbbbbbbbb\u4E2Dbbbbbbbb", "\uFEFF\u4E2Dbbbbbbbb\u03A9bbbbbbbb", "\uFEFFb\u4E2Db\u03A9b\uFEFFbbbbbbbb", "\uFEFF\u0100" + new string('b', 100), "\uFEFF\0" + new string('b', 100), "\uFEFF\uD83D\uDE00" + new string('b', 100), "\uFEFF" + new string('\u4E2D', 5) + new string('b', 4), "\uFEFF" + new string('\u4E2D', 5) + new string('b', 3), "\uFEFF" + new string('b', 242), "\uFEFF" + new string('\u4E2D', 238) + "tail"];

    private static async Task<List<byte[]>> ReadPayloadsAsync(AccessReader reader)
    {
        long owner = Assert.IsType<JetDatabaseWriter.Catalog.Models.CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
        var session = new CompactDiscoverySession(reader.Database.Pages, reader.Database.Format, new());
        var payloads = new List<byte[]>();
        await session.ScanRowsAsync(
            owner,
            (location, row, token) => CompactRawFieldReader.ScanAsync(
                reader.Database.Format,
                image,
                location,
                row,
                (field, _) =>
                {
                    if (field.Column.Name == "Cached")
                    {
                        payloads.Add(CalculatedColumnUtil.Unwrap(field.RawBytes.ToArray()));
                    }

                    return default;
                },
                token),
            Ct);
        return payloads;
    }
}
