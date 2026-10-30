namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native compressed Text storage preserves a literal leading Unicode marker.</summary>
/// <param name="output">Records native interoperability evidence.</param>
public sealed class NativeCompressedTextMarkerTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("native-compressed.mdb", "273A0B52E5D628D05CFF57749A7EB0D7ADA69202842E2B22840131AB5777E9DD")]
    [InlineData("native-compressed-compacted.mdb", "39C7378259246B61A6A18D720C001C6A7972A05D3432F7B7D76D8197783B18E2")]
    public async Task FrozenNativeCompressedText_ReadsExactMarkerAndNativeStorage(string name, string hash)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "NativeTextMarkers", name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        string[] expected = ["\uFEFF" + new string('b', 5), "\uFEFF" + new string('b', 39), "\uFEFF" + new string('b', 254), "\uFEFFtail!", "a", "\uFEFF\u4E2Dtail"];
        await using var stream = new MemoryStream(bytes, writable: false);
        await AssertReadSurfacesAsync(stream, expected);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        List<byte[]> payloads = await ReadRawTextAsync(reader);
        Assert.Contains(payloads, static payload => payload.AsSpan().SequenceEqual(new byte[] { 0xFF, 0xFE, 0, 0xFF, 0xFE, 0, 0x62, 0x62, 0x62, 0x62, 0x62 }));
        Assert.Contains(payloads, static payload => payload.AsSpan().SequenceEqual(new byte[] { 0xFF, 0xFE, 0, 0xFF, 0xFE, 0, 0x74, 0x61, 0x69, 0x6C, 0x21 }));
        Assert.Contains(payloads, static payload => payload.AsSpan().SequenceEqual(new byte[] { 0xFF, 0xFE, 0, 0xFF, 0xFE, 0x2D, 0x4E, 0, 0x74, 0x61, 0x69, 0x6C }));
        Assert.Contains(payloads, static payload => payload.AsSpan().SequenceEqual(new byte[] { 0xFF, 0xFE, 0x61, 0 }));
        Assert.Equal(bytes, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task WriterCompressedText_LeadingMarkerSurvivesInsertUpdateAndSchemaRewrite(DatabaseFormat kind)
    {
        string[] expected = Values();
        await using MemoryStream stream = await CreateAsync(kind, expected);
        await AssertReadSurfacesAsync(stream, expected);
        await using (AccessReader beforeEdit = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            List<byte[]> actual = await ReadRawTextAsync(beforeEdit);
            string nativePath = Path.Combine(AppContext.BaseDirectory, "Databases", "NativeTextMarkers", "native-compressed.mdb");
            await using AccessReader native = await TestDatabase.OpenReaderAsync(nativePath, cancellationToken: Ct);
            List<byte[]> nativePayloads = await ReadRawTextAsync(native);
            foreach (byte[] payload in nativePayloads.Where(static payload => payload.Length > 2 && payload[2] == 0))
            {
                Assert.Contains(actual, field => field.AsSpan().SequenceEqual(payload));
            }
        }

        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 0, new Dictionary<string, object?> { ["Content"] = "\uFEFF\u4E2Dtail" }, Ct));
            await writer.RenameColumnAsync("Items", "Content", "Renamed", Ct);
            await writer.RenameColumnAsync("Items", "Renamed", "Content", Ct);
        }

        expected[0] = "\uFEFF\u4E2Dtail";
        await AssertReadSurfacesAsync(stream, expected);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        List<byte[]> payloads = await ReadRawTextAsync(reader);
        Assert.Contains(payloads, static payload => payload.AsSpan().SequenceEqual(new byte[] { 0xFF, 0xFE, 0, 0xFF, 0xFE, 0, 0x74, 0x61, 0x69, 0x6C, 0x21 }));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, "\uFEFF")]
    [InlineData(DatabaseFormat.Jet4Mdb, "\uFEFFtail")]
    [InlineData(DatabaseFormat.Jet4Mdb, "\uFEFF\0tail!")]
    [InlineData(DatabaseFormat.Jet4Mdb, "\uFEFF\u0100tail")]
    [InlineData(DatabaseFormat.Jet4Mdb, "\uFEFF\u4E2Dabc")]
    [InlineData(DatabaseFormat.Jet4Mdb, "\uFEFFtail\u4E2D")]
    [InlineData(DatabaseFormat.Jet4Mdb, "\uFEFF\uD83D\uDE00tail")]
    [InlineData(DatabaseFormat.AceAccdb, "\uFEFF")]
    [InlineData(DatabaseFormat.AceAccdb, "\uFEFFtail")]
    [InlineData(DatabaseFormat.AceAccdb, "\uFEFF\0tail!")]
    [InlineData(DatabaseFormat.AceAccdb, "\uFEFF\u0100tail")]
    [InlineData(DatabaseFormat.AceAccdb, "\uFEFF\u4E2Dabc")]
    [InlineData(DatabaseFormat.AceAccdb, "\uFEFFtail\u4E2D")]
    [InlineData(DatabaseFormat.AceAccdb, "\uFEFF\uD83D\uDE00tail")]
    public async Task UnsupportedMarkerShape_RefusesBeforeOtherLongValuesAreAllocated(DatabaseFormat kind, string value)
        => await AssertRefusalUnchangedAsync(kind, value, 255, JetErrorCode.FeatureNotSupported);

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DeclaredTextLimit_RefusesWholeEscapeWithoutTruncation(DatabaseFormat kind)
    {
        await using MemoryStream stream = await CreateAsync(kind, ["\uFEFFtail!"], 6);
        await AssertReadSurfacesAsync(stream, ["\uFEFFtail!"]);
        await AssertRefusalUnchangedAsync(kind, "\uFEFFtail!!", 6, JetErrorCode.ValueTooLarge);
        await AssertRefusalUnchangedAsync(kind, "\uFEFFtail!", 5, JetErrorCode.ValueTooLarge);
        await AssertRefusalUnchangedAsync(kind, "\uFEFF" + new string('b', 255), 255, JetErrorCode.ValueTooLarge);
        await using MemoryStream wide = await CreateAsync(kind, ["\uFEFF\u4E2Dtail"], 6);
        await AssertReadSurfacesAsync(wide, ["\uFEFF\u4E2Dtail"]);
        await AssertRefusalUnchangedAsync(kind, "\uFEFF\u4E2Dtail!", 6, JetErrorCode.ValueTooLarge);
        await AssertRefusalUnchangedAsync(kind, "\uFEFF\u4E2Dtail", 5, JetErrorCode.ValueTooLarge);
        await AssertRefusalUnchangedAsync(kind, "\uFEFF\u4E2D" + new string('b', 254), 255, JetErrorCode.ValueTooLarge);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task WriterCompressedText_Dao36ReadsUpdatesAndCompactsExactValues()
        => await this.AssertNativeWriterAsync(DatabaseFormat.Jet4Mdb, dao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task WriterCompressedText_Dao120ReadsUpdatesAndCompactsExactValues(DatabaseFormat kind)
        => await this.AssertNativeWriterAsync(kind, dao36: false);

    private static string[] Values()
        => ["\uFEFF" + new string('b', 5), "\uFEFF" + new string('b', 39), "\uFEFF" + new string('b', 254), "\uFEFFtail!", "\uFEFF" + new string(Enumerable.Range(1, 254).Select(static value => (char)value).ToArray()), "\uFEFF" + new string('\u00FF', 5), "\uFEFF\u4E2Dtail", "\uFEFF\u03A9tail", "\uFEFF\u4E2D\u03A9tail", "\uFEFF" + new string('\u4E2D', 250) + "tail", "\uFEFF\u4E2D" + new string('b', 253)];

    private static async Task<MemoryStream> CreateAsync(DatabaseFormat kind, string[] values, int maxLength = 255)
    {
        var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Content", typeof(string), maxLength), new("Memo", typeof(string), 20_000)], Ct);
        await writer.InsertRowsAsync("Items", values.Select(static (value, id) => new object?[] { id, value, "retained" }), Ct);
        return stream;
    }

    private static async Task AssertRefusalUnchangedAsync(DatabaseFormat kind, string value, int maxLength, JetErrorCode code)
    {
        await using MemoryStream stream = await CreateAsync(kind, ["old"], maxLength);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            JetLimitationException inserted = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.InsertRowAsync("Items", [2, value, new string('m', 5000)], Ct));
            Assert.Equal(code, inserted.ErrorCode);
            Assert.Equal("Items", inserted.ErrorInfo.TableName);
            Assert.Equal("Content", inserted.ErrorInfo.ColumnName);
            JetLimitationException updated = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.UpdateRowsAsync("Items", "Id", 0, new Dictionary<string, object?> { ["Memo"] = new string('m', 5000), ["Content"] = value }, Ct));
            Assert.Equal(code, updated.ErrorCode);
            Assert.Equal("Items", updated.ErrorInfo.TableName);
            Assert.Equal("Content", updated.ErrorInfo.ColumnName);
        }

        Assert.Equal(before, stream.ToArray());
    }

    private static async Task AssertReadSurfacesAsync(MemoryStream stream, string[] expected)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        List<object[]> typed = await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct);
        List<string[]> strings = await reader.RowsAsStrings("Items", cancellationToken: Ct).ToListAsync(Ct);
        List<TextRow> pocos = await reader.Rows<TextRow>("Items", cancellationToken: Ct).ToListAsync(Ct);
        using System.Data.DataTable table = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(expected, typed.OrderBy(static row => (int)row[0]).Select(static row => Assert.IsType<string>(row[1])));
        Assert.Equal(expected, strings.OrderBy(static row => int.Parse(row[0], System.Globalization.CultureInfo.InvariantCulture)).Select(static row => row[1]));
        Assert.Equal(expected, pocos.OrderBy(static row => row.Id).Select(static row => row.Content));
        Assert.Equal(expected, table.Rows.Cast<System.Data.DataRow>().OrderBy(static row => (int)row["Id"]).Select(static row => Assert.IsType<string>(row["Content"])));
    }

    private static async Task<List<byte[]>> ReadRawTextAsync(AccessReader reader)
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
                    if (field.Column.Name == "Content")
                    {
                        Assert.True(field.Column.IsCompressedUnicode);
                        payloads.Add(field.RawBytes.ToArray());
                    }

                    return default;
                },
                token),
            Ct);
        return payloads;
    }

    private async Task AssertNativeWriterAsync(DatabaseFormat kind, bool dao36)
    {
        string[] values = Values();
        await using MemoryStream stream = await CreateAsync(kind, values);
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompressedTextMarker", databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await File.WriteAllBytesAsync(native.SourcePath, stream.ToArray(), Ct);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string authoredPath = Path.Combine(native.WorkDir, "native-authored" + Path.GetExtension(native.SourcePath));
        string authoredCompactedPath = Path.Combine(native.WorkDir, "native-authored-compacted" + Path.GetExtension(native.SourcePath));
        await using (MemoryStream empty = await CreateAsync(kind, []))
        {
            await File.WriteAllBytesAsync(authoredPath, empty.ToArray(), Ct);
        }

        string authored = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(authoredPath);
        string authoredCompacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(authoredCompactedPath);
        string encoded = string.Join(',', values.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(value)))));
        string script = $$"""
            Write-Output "ENGINE=$($engine.Version)"
            Get-Process -Id $PID | ForEach-Object { $_.Modules } | Where-Object { $_.ModuleName -match '^(dao360|acedao)\.dll$' } | ForEach-Object {
                Write-Output "DAO_DLL=$($_.FileName) VERSION=$([Diagnostics.FileVersionInfo]::GetVersionInfo($_.FileName).FileVersion)"
            }
            $values=@({{encoded}})
            function Assert-Values([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('SELECT Id,Content FROM Items ORDER BY Id',2)
                    try {
                        $id=0
                        while(!$rs.EOF) {
                            $expected=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id]))
                            if($rs.Fields.Item('Id').Value -ne $id -or ![string]::Equals([string]$rs.Fields.Item('Content').Value,$expected,[StringComparison]::Ordinal)) { throw "Native compressed Text mismatch at $id" }
                            $id++
                            $rs.MoveNext()
                        }
                        if($id -ne $values.Count) { throw 'Native compressed Text row count changed' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            $db=$engine.OpenDatabase({{authored}})
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
            Assert-Values {{authored}}
            $engine.CompactDatabase({{authored}},{{authoredCompacted}})
            Assert-Values {{authoredCompacted}}
            Assert-Values {{source}}
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('SELECT * FROM Items WHERE Id=0',2)
                try { $rs.Edit(); $rs.Fields.Item('Content').Value=[string][Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[6])); $rs.Update() } finally { $rs.Close() }
            } finally { $db.Close() }
            $values[0]=$values[6]
            Assert-Values {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Values {{compacted}}
            """;
        if (dao36)
        {
            JetDatabaseWriter.TestSupport.PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, Path.GetDirectoryName(native.SourcePath)!, TimeSpan.FromMinutes(2));
            output.WriteLine(result.StandardOutput);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            output.WriteLine(result.StdOut);
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }

        string[] expectedPayloads = values.Select(static value => Convert.ToHexString(JetFormat.EncodeEscapedVariableText(value, 510))).Order(StringComparer.Ordinal).ToArray();
        foreach (string path in new[] { authoredPath, authoredCompactedPath })
        {
            await using AccessReader nativeRows = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
            List<byte[]> payloads = await ReadRawTextAsync(nativeRows);
            Assert.Equal(values.Length, payloads.Count);
            Assert.Equal(expectedPayloads, payloads.Select(static payload => Convert.ToHexString(payload)).Order(StringComparer.Ordinal));
        }

        values[0] = values[6];
        foreach (string path in new[] { native.SourcePath, native.CompactedPath })
        {
            await using var actual = new MemoryStream(await File.ReadAllBytesAsync(path, Ct), writable: false);
            await AssertReadSurfacesAsync(actual, values);
        }
    }

#pragma warning disable CA1812 // Public POCO materialization constructs this projection target.
    private sealed class TextRow
    {
        public int Id { get; set; }

        public string Content { get; set; } = string.Empty;
    }
#pragma warning restore CA1812
}
